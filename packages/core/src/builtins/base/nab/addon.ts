import { z } from 'zod';
import { ParsedId } from '../../../utils/id-parser.js';
import { Env, getTimeTakenSincePoint } from '../../../utils/index.js';
import { Logger } from 'winston';
import {
  BaseDebridAddon,
  BaseDebridConfigSchema,
  SearchMetadata,
} from '../debrid.js';
import {
  BaseNabApi,
  Capabilities,
  JackettIndexer,
  SearchResponse,
  SearchResultItem,
} from './api.js';
import { useAllTitles } from '../../utils/general.js';
import { collectUntilDeadline } from '../../utils/deadline.js';

export const NabAddonConfigSchema = BaseDebridConfigSchema.extend({
  url: z.string(),
  apiKey: z.string().optional(),
  apiPath: z.string().optional(),
  searchTimeout: z.number().optional(),
  forceQuerySearch: z.boolean().default(false),
  legacyForceQuerySearch: z.boolean().optional().default(false),
  paginate: z.boolean().default(false),
  forceInitialLimit: z.number().optional(),
});
export type NabAddonConfig = z.infer<typeof NabAddonConfigSchema>;

interface SearchResultMetadata {
  searchType: 'id' | 'query';
  capabilities: Capabilities;
}

export abstract class BaseNabAddon<
  C extends NabAddonConfig,
  A extends BaseNabApi<'torznab' | 'newznab'>,
> extends BaseDebridAddon<C> {
  abstract api: A;

  private static readonly JACKETT_INDEXER_CACHE_TTL_MS = 5 * 60 * 1000;
  private static readonly JACKETT_INDEXER_QUARANTINE_MS = 10 * 60 * 1000;
  private static readonly JACKETT_INDEXER_FAILURE_THRESHOLD = 3;

  private static readonly jackettIndexersCache = new Map<
    string,
    { expiresAt: number; indexers: JackettIndexer[] }
  >();

  private static readonly jackettIndexerHealth = new Map<
    string,
    { failures: number; quarantinedUntil: number }
  >();

  private shouldForceQuerySearch(): boolean {
    const isJackettAllIndexers = this.isJackettAllIndexersUrl();

    if (!isJackettAllIndexers) {
      return this.userData.forceQuerySearch;
    }

    if (this.userData.legacyForceQuerySearch) {
      return true;
    }

    return false;
  }

  private isJackettAllIndexersUrl(): boolean {
    return this.userData.url
      .toLowerCase()
      .includes('/api/v2.0/indexers/all/results/torznab');
  }

  private normalizeIndexerId(indexerId: string): string {
    return indexerId.trim().toLowerCase();
  }

  private getIndexerHealthKey(indexerId: string): string {
    return `${this.userData.url.toLowerCase()}::${this.normalizeIndexerId(indexerId)}`;
  }

  private isIndexerQuarantined(indexerId: string): boolean {
    const key = this.getIndexerHealthKey(indexerId);
    const health = BaseNabAddon.jackettIndexerHealth.get(key);
    if (!health) return false;

    if (health.quarantinedUntil > Date.now()) return true;

    BaseNabAddon.jackettIndexerHealth.delete(key);
    return false;
  }

  private markIndexerSuccess(indexerId: string): void {
    const key = this.getIndexerHealthKey(indexerId);
    BaseNabAddon.jackettIndexerHealth.delete(key);
  }

  private markIndexerFailure(indexerId: string, reason: unknown): void {
    const key = this.getIndexerHealthKey(indexerId);
    const current =
      BaseNabAddon.jackettIndexerHealth.get(key) ?? {
        failures: 0,
        quarantinedUntil: 0,
      };
    current.failures += 1;

    if (current.failures >= BaseNabAddon.JACKETT_INDEXER_FAILURE_THRESHOLD) {
      current.failures = 0;
      current.quarantinedUntil =
        Date.now() + BaseNabAddon.JACKETT_INDEXER_QUARANTINE_MS;
      this.logger.warn('Quarantining Jackett indexer after repeated failures', {
        indexerId,
        quarantineMs: BaseNabAddon.JACKETT_INDEXER_QUARANTINE_MS,
        reason: reason instanceof Error ? reason.message : String(reason),
      });
    }

    BaseNabAddon.jackettIndexerHealth.set(key, current);
  }

  private async getJackettIndexers(): Promise<JackettIndexer[]> {
    const cacheKey = this.userData.url.toLowerCase();
    const cached = BaseNabAddon.jackettIndexersCache.get(cacheKey);
    const now = Date.now();
    if (cached && cached.expiresAt > now) {
      return cached.indexers;
    }

    const indexers = await this.api.getJackettIndexers(true);
    BaseNabAddon.jackettIndexersCache.set(cacheKey, {
      indexers,
      expiresAt: now + BaseNabAddon.JACKETT_INDEXER_CACHE_TTL_MS,
    });
    return indexers;
  }

  private buildJackettIndexerApiPath(indexerId: string): string {
    const parsedUrl = new URL(this.userData.url);
    const normalizedPath = parsedUrl.pathname.replace(/\/+$/, '');
    const replacedPath = normalizedPath.replace(
      /\/indexers\/all\/results\/torznab(?:\/api)?$/,
      `/indexers/${encodeURIComponent(indexerId)}/results/torznab/api`
    );

    if (replacedPath === normalizedPath) {
      return this.userData.url;
    }

    parsedUrl.pathname = replacedPath;
    parsedUrl.search = '';
    parsedUrl.hash = '';
    return parsedUrl.toString();
  }

  private prioritizeQueries(parsedId: ParsedId, queries: string[]): string[] {
    const uniqueQueries = [...new Set(queries)];

    if (
      parsedId.mediaType !== 'series' ||
      parsedId.season === undefined ||
      parsedId.episode === undefined
    ) {
      return uniqueQueries;
    }

    const season = parsedId.season.toString().padStart(2, '0');
    const episode = parsedId.episode.toString().padStart(2, '0');
    const fullPattern = new RegExp(`S${season}E${episode}`, 'i');
    const seasonPattern = new RegExp(`S${season}`, 'i');

    return uniqueQueries.sort((left, right) => {
      const score = (query: string) => {
        if (fullPattern.test(query)) return 0;
        if (seasonPattern.test(query)) return 1;
        return 2;
      };

      return score(left) - score(right);
    });
  }

  private async searchJackettByIndexer(
    searchFunction: string,
    baseParams: Record<string, string>,
    queries: string[],
    searchTimeout?: number,
    parsedId?: ParsedId
  ): Promise<SearchResultItem<A['namespace']>[]> {
    let availableIndexers: JackettIndexer[] = [];
    try {
      availableIndexers = await this.getJackettIndexers();
    } catch (error) {
      this.logger.warn(
        `Could not fetch Jackett indexers, falling back to aggregate query mode: ${error instanceof Error ? error.message : String(error)}`
      );
      const fallbackPromises = queries.map((q) =>
        this.fetchResults(searchFunction, { ...baseParams, q }, searchTimeout)
      );
      const fallbackCollected = await collectUntilDeadline(
        fallbackPromises,
        searchTimeout
      );
      return fallbackCollected.fulfilled.flatMap((result) => result);
    }

    if (availableIndexers.length === 0) {
      return [];
    }

    const start = Date.now();
    const allResults: SearchResultItem<A['namespace']>[] = [];
    const prioritizedQueries =
      parsedId !== undefined ? this.prioritizeQueries(parsedId, queries) : queries;

    for (const query of prioritizedQueries) {
      const elapsed = Date.now() - start;
      const remaining =
        searchTimeout === undefined ? undefined : Math.max(0, searchTimeout - elapsed);
      if (remaining !== undefined && remaining <= 0) {
        break;
      }

      const healthyIndexers = availableIndexers.filter(
        (indexer) => !this.isIndexerQuarantined(indexer.id)
      );
      const chosenIndexers =
        healthyIndexers.length > 0 ? healthyIndexers : availableIndexers;

      const taskMeta = chosenIndexers.map((indexer) => ({
        indexerId: indexer.id,
        query,
      }));
      const searchPromises = taskMeta.map((meta) =>
        this.api
          .searchWithApiPath(
            searchFunction,
            {
              ...baseParams,
              q: meta.query,
            },
            this.buildJackettIndexerApiPath(meta.indexerId),
            remaining
          )
          .then((response) => ({
            indexerId: meta.indexerId,
            results: response.results as SearchResultItem<A['namespace']>[],
          }))
      );

      const collected = await collectUntilDeadline(searchPromises, remaining);

      for (const fulfilled of collected.fulfilled) {
        this.markIndexerSuccess(fulfilled.indexerId);
        if (fulfilled.results.length > 0) {
          allResults.push(...fulfilled.results);
        }
      }

      for (const rejected of collected.rejected) {
        const failedTask = taskMeta[rejected.index];
        if (failedTask) {
          this.markIndexerFailure(failedTask.indexerId, rejected.reason);
        }
      }

      for (const pendingIndex of collected.pendingIndexes) {
        const pendingTask = taskMeta[pendingIndex];
        if (pendingTask) {
          this.markIndexerFailure(
            pendingTask.indexerId,
            `query deadline reached for ${pendingTask.query}`
          );
        }
      }

      if (collected.failed > 0 || collected.pendingAtDeadline > 0) {
        this.logger.warn('Jackett per-indexer query returned partial results', {
          query,
          failedRequests: collected.failed,
          pendingAtDeadline: collected.pendingAtDeadline,
          totalRequests: collected.total,
          timedOut: collected.timedOut,
          selectedIndexers: chosenIndexers.length,
          quarantinedIndexers:
            availableIndexers.length - healthyIndexers.length,
        });
      }

      if (parsedId?.mediaType === 'series' && allResults.length > 0) {
        break;
      }
    }

    return allResults;
  }

  protected async performSearch(
    parsedId: ParsedId,
    metadata: SearchMetadata
  ): Promise<{
    results: SearchResultItem<A['namespace']>[];
    meta: SearchResultMetadata;
  }> {
    const forceQuerySearch = this.shouldForceQuerySearch();
    const forceIncludeSeasonEpInParams = ['StremThru'];
    const start = Date.now();
    const queryParams: Record<string, string> = {};
    let capabilities: Capabilities;
    let searchType: SearchResultMetadata['searchType'] = 'id';
    try {
      capabilities = await this.api.getCapabilities();
    } catch (error) {
      throw new Error(
        `Could not get capabilities: ${error instanceof Error ? error.message : String(error)}`
      );
    }

    this.logger.debug(`Capabilities: ${JSON.stringify(capabilities)}`);

    const chosenFunction = this.getSearchFunction(
      parsedId.mediaType,
      capabilities.searching
    );
    if (!chosenFunction)
      throw new Error(
        `Could not find a search function for ${capabilities.server.title}`
      );

    const { capabilities: searchCapabilities, function: searchFunction } =
      chosenFunction;
    this.logger.debug(`Using search function: ${searchFunction}`, {
      searchCapabilities,
    });

    queryParams.limit =
      this.userData.forceInitialLimit?.toString() ??
      capabilities.limits?.max?.toString() ??
      '10000';

    if (forceQuerySearch) {
    } else if (
      // prefer tvdb ID over imdb ID for series
      parsedId.mediaType === 'series' &&
      searchCapabilities.supportedParams.includes('tvdbid') &&
      metadata.tvdbId
    ) {
      queryParams.tvdbid = metadata.tvdbId.toString();
    } else if (
      searchCapabilities.supportedParams.includes('imdbid') &&
      metadata.imdbId
    )
      queryParams.imdbid = metadata.imdbId.replace('tt', '');
    else if (
      searchCapabilities.supportedParams.includes('tmdbid') &&
      metadata.tmdbId
    )
      queryParams.tmdbid = metadata.tmdbId.toString();
    else if (
      searchCapabilities.supportedParams.includes('tvdbid') &&
      metadata.tvdbId
    )
      queryParams.tvdbid = metadata.tvdbId.toString();

    if (
      ((!forceQuerySearch &&
        searchCapabilities.supportedParams.includes('season')) ||
        forceIncludeSeasonEpInParams.includes(
          capabilities.server.title || ''
        )) &&
      parsedId.season
    )
      queryParams.season = parsedId.season.toString();
    if (
      ((!forceQuerySearch &&
        searchCapabilities.supportedParams.includes('ep')) ||
        forceIncludeSeasonEpInParams.includes(
          capabilities.server.title || ''
        )) &&
      parsedId.episode
    )
      queryParams.ep = parsedId.episode.toString();
    if (
      !forceQuerySearch &&
      searchCapabilities.supportedParams.includes('year') &&
      metadata.year &&
      parsedId.mediaType === 'movie'
    )
      queryParams.year = metadata.year.toString();

    let queries: string[] = [];
    if (
      !queryParams.imdbid &&
      !queryParams.tmdbid &&
      !queryParams.tvdbid &&
      searchCapabilities.supportedParams.includes('q') &&
      metadata.primaryTitle
    ) {
      queries = this.buildQueries(parsedId, metadata, {
        // add year if it is not already in the query params
        addYear: !queryParams.year,
        // add season and episode if they are not already in the query params
        // some endpoints won't return results with season/ep in query
        addSeasonEpisode: forceIncludeSeasonEpInParams.includes(
          capabilities.server.title || ''
        )
          ? false
          : !queryParams.season && !queryParams.ep,
        useAllTitles: useAllTitles(this.userData.url),
      });
      searchType = 'query';
    }
    queryParams.extended = '1';
    const searchTimeout = this.userData.searchTimeout
      ? Math.max(Env.MIN_TIMEOUT, this.userData.searchTimeout - 250)
      : undefined;
    let results: SearchResultItem<A['namespace']>[] = [];
    if (queries.length > 0) {
      queries = this.prioritizeQueries(parsedId, queries);
      this.logger.debug('Performing queries', { queries });

      if (this.isJackettAllIndexersUrl()) {
        results = await this.searchJackettByIndexer(
          searchFunction,
          queryParams,
          queries,
          searchTimeout,
          parsedId
        );
      } else {
        const searchPromises = queries.map((q) =>
          this.fetchResults(searchFunction, { ...queryParams, q }, searchTimeout)
        );
        const collected = await collectUntilDeadline(searchPromises, searchTimeout);

        if (collected.failed > 0 || collected.pendingAtDeadline > 0) {
          this.logger.warn(`Nab search returned partial query results.`, {
            failedQueries: collected.failed,
            pendingAtDeadline: collected.pendingAtDeadline,
            totalQueries: collected.total,
            timedOut: collected.timedOut,
          });
        }

        results = collected.fulfilled.flatMap((result) => result);
      }
    } else {
      results = await this.fetchResults(
        searchFunction,
        queryParams,
        searchTimeout
      );
    }
    this.logger.info(
      `Completed search for ${capabilities.server.title} in ${getTimeTakenSincePoint(start)}`,
      {
        results: results.length,
      }
    );
    return {
      results: results,
      meta: {
        searchType,
        capabilities,
      },
    };
  }

  private getSearchFunction(
    type: string,
    searching: Capabilities['searching']
  ) {
    const available = Object.keys(searching);
    this.logger.debug(
      `Available search functions: ${JSON.stringify(available)}`
    );
    if (this.userData.forceQuerySearch) {
      // dont use specific search functions when force query search is enabled
    } else if (type === 'movie') {
      const movieSearch = available.find((s) =>
        s.toLowerCase().includes('movie')
      );
      if (movieSearch && searching[movieSearch].available)
        return {
          capabilities: searching[movieSearch],
          function: 'movie',
        };
    } else {
      const tvSearch = available.find((s) => s.toLowerCase().includes('tv'));
      if (tvSearch && searching[tvSearch].available)
        return {
          capabilities: (searching as any)[tvSearch],
          function: 'tvsearch',
        };
    }
    if ((searching as any).search.available)
      return { capabilities: (searching as any).search, function: 'search' };
    return undefined;
  }

  private async fetchResults(
    searchFunction: string,
    params: Record<string, string>,
    timeout?: number
  ): Promise<SearchResultItem<A['namespace']>[]> {
    const maxPages = Env.BUILTIN_NAB_MAX_PAGES;

    const initialResponse: SearchResponse<A['namespace']> =
      await this.api.search(searchFunction, params, timeout);
    let allResults = [...initialResponse.results];

    this.logger.debug('Initial search response', {
      resultsCount: initialResponse.results.length,
      offset: initialResponse.offset,
      total: initialResponse.total,
    });

    // if both first and last items are duplicates, the page is likely a duplicate
    const areResultsDuplicate = (
      existing: SearchResultItem<A['namespace']>[],
      newResults: SearchResultItem<A['namespace']>[]
    ): boolean => {
      if (newResults.length === 0) return false;

      const firstNew = newResults[0];
      const lastNew = newResults[newResults.length - 1];

      const firstExists = existing.some((r) => r.guid === firstNew.guid);
      const lastExists = existing.some((r) => r.guid === lastNew.guid);

      return firstExists && lastExists;
    };

    if (!this.userData.paginate) {
      this.logger.info(
        'Pagination handling is disabled, returning initial results only'
      );
      return allResults;
    }

    if (initialResponse.total !== undefined && initialResponse.total > 0) {
      const limit =
        initialResponse.results.length > 0
          ? initialResponse.results.length
          : parseInt(params.limit || '100', 10);
      const total = initialResponse.total;
      const initialOffset = initialResponse.offset || 0;

      // Calculate how many more pages we need
      const remainingResults = total - (initialOffset + limit);
      if (remainingResults > 0) {
        const additionalPages = Math.ceil(remainingResults / limit);
        const pagesToFetch = Math.min(additionalPages, maxPages - 1); // -1 because we already fetched first page

        if (pagesToFetch > 0) {
          this.logger.debug('Fetching additional pages with known total', {
            total,
            limit,
            pagesToFetch,
            remainingResults,
          });

          // Create requests for all remaining pages in parallel
          const pagePromises = Array.from({ length: pagesToFetch }, (_, i) => {
            const offset = initialOffset + limit * (i + 1);
            return this.api.search(searchFunction, {
              ...params,
              offset: offset.toString(),
            }, timeout) as Promise<SearchResponse<A['namespace']>>;
          });

          const settledPageResponses = await Promise.allSettled(pagePromises);
          const failedPages = settledPageResponses.filter(
            (result) => result.status === 'rejected'
          );
          if (failedPages.length > 0) {
            this.logger.warn(
              `Nab pagination had ${failedPages.length}/${pagePromises.length} failed page requests; returning partial page results.`
            );
          }

          for (const response of settledPageResponses) {
            if (response.status !== 'fulfilled') continue;

            if (areResultsDuplicate(allResults, response.value.results)) {
              this.logger.warn(
                'Detected duplicate results in paginated response. Indexer may not support offset parameter despite claiming support. Stopping pagination.'
              );
              break;
            }
            allResults.push(...response.value.results);
          }
        }
      }
    } else {
      // keep fetching until we get empty results or hit max pages
      let pageCount = 1;
      let currentOffset =
        (initialResponse.offset || 0) + initialResponse.results.length;
      const limit =
        initialResponse.results.length > 0
          ? initialResponse.results.length
          : parseInt(params.limit || '100', 10);

      this.logger.debug('Fetching pages without known total', {
        initialResultsCount: initialResponse.results.length,
        limit,
      });

      while (pageCount < maxPages) {
        const response: SearchResponse<A['namespace']> = await this.api.search(
          searchFunction,
          {
            ...params,
            offset: currentOffset.toString(),
          },
          timeout
        );

        if (response.results.length === 0) {
          this.logger.debug('Received empty page, stopping pagination');
          break;
        }

        if (areResultsDuplicate(allResults, response.results)) {
          this.logger.warn(
            'Detected duplicate results in paginated response. Indexer may not support offset parameter. Stopping pagination.'
          );
          break;
        }

        allResults.push(...response.results);
        currentOffset += response.results.length;
        pageCount++;

        this.logger.debug('Fetched additional page', {
          pageCount,
          resultsInPage: response.results.length,
          totalResults: allResults.length,
        });

        // if this page returned less results than the limit, we can assume there are no more pages
        if (response.results.length < limit) {
          this.logger.debug(
            'Received less results than limit, assuming last page'
          );
          break;
        }
      }

      if (pageCount >= maxPages) {
        this.logger.warn(
          `Reached maximum page limit (${maxPages}), stopping pagination`
        );
      }
    }

    this.logger.info('Completed fetching all results', {
      totalResults: allResults.length,
    });

    return allResults;
  }
}
