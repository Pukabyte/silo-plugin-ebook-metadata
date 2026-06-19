package provider

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/Silo-Server/silo-plugin-ebook-metadata/metadata"
)

const (
	providerTimeout = 10 * time.Second
	searchWorkers   = 4
)

type Source interface {
	ID() string
	Search(context.Context, metadata.SearchQuery) ([]metadata.Match, error)
	Fetch(context.Context, string) (*metadata.Match, error)
}

type Provider struct {
	sources []Source
	byID    map[string]Source
}

type Options struct {
	EnabledSources    []string
	GoogleBooksAPIKey string
	ISBNdbAPIKey      string
	HardcoverAPIKey   string
	DefaultRegion     string
}

func NewProvider() *Provider {
	return NewProviderWithOptions(Options{})
}

func NewProviderWithOptions(options Options) *Provider {
	return NewProviderWithSources(defaultSources(options))
}

func NewProviderWithSources(sources []Source) *Provider {
	p := &Provider{
		sources: make([]Source, 0, len(sources)),
		byID:    make(map[string]Source, len(sources)),
	}

	for _, source := range sources {
		if source == nil {
			continue
		}
		id := strings.TrimSpace(source.ID())
		if id == "" {
			continue
		}
		p.sources = append(p.sources, source)
		p.byID[id] = source
	}

	return p
}

func (p *Provider) Search(ctx context.Context, q metadata.SearchQuery) ([]metadata.Match, error) {
	tctx, cancel := context.WithTimeout(ctx, providerTimeout)
	defer cancel()

	type result struct {
		source  string
		matches []metadata.Match
		err     error
	}

	results := make(chan result, len(p.sources))
	sem := make(chan struct{}, searchWorkers)

	var wg sync.WaitGroup
	for _, source := range p.sources {
		wg.Add(1)
		go func(source Source) {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
			case <-tctx.Done():
				results <- result{source: strings.TrimSpace(source.ID()), err: tctx.Err()}
				return
			}
			defer func() { <-sem }()

			matches, err := source.Search(tctx, q)
			results <- result{
				source:  strings.TrimSpace(source.ID()),
				matches: matches,
				err:     err,
			}
		}(source)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var matches []metadata.Match
	for result := range results {
		if result.err != nil {
			log.Printf("ebook-metadata: provider %s search error: %v", result.source, result.err)
			continue
		}
		matches = append(matches, result.matches...)
	}

	return matches, nil
}

func (p *Provider) Fetch(ctx context.Context, q metadata.SearchQuery) (*metadata.Match, error) {
	tctx, cancel := context.WithTimeout(ctx, providerTimeout)
	defer cancel()

	match, backfill, err := p.fetchPrimary(tctx, q)
	// Backfill only on the ISBN-fallback path: an explicit source-specific or
	// capability ID pins a single source and must not fan out to others.
	if match != nil && backfill {
		p.backfillCover(tctx, match, q)
	}
	return match, err
}

// fetchPrimary resolves the best metadata match using provider-specific IDs,
// then the capability ID, then ISBN. It returns the first source that responds
// regardless of whether that match carries a cover. The bool reports whether
// the match came from the ISBN fallback (i.e. no explicit source pin), in which
// case the caller may backfill a missing cover from other sources.
func (p *Provider) fetchPrimary(tctx context.Context, q metadata.SearchQuery) (*metadata.Match, bool, error) {
	for _, source := range p.sources {
		sourceID := strings.TrimSpace(source.ID())
		providerID := strings.TrimSpace(q.ProviderIDs[sourceID])
		if sourceID == "" || providerID == "" {
			continue
		}
		match, err := source.Fetch(tctx, providerID)
		return match, false, err
	}

	if sourceID, providerID := metadata.ParseCapabilityProviderID(q.ProviderIDs[metadata.CapabilityID]); sourceID != "" {
		if source := p.byID[sourceID]; source != nil {
			match, err := source.Fetch(tctx, providerID)
			return match, false, err
		}
	}

	isbn := metadata.NormalizeISBN(q.ProviderIDs["isbn"])
	if isbn == "" {
		return nil, false, nil
	}
	var lastErr error
	for _, sourceID := range []string{"openlibrary", "googlebooks", "isbndb"} {
		source := p.byID[sourceID]
		if source == nil {
			continue
		}
		match, err := source.Fetch(tctx, isbn)
		if match != nil {
			return match, true, nil
		}
		if err != nil {
			lastErr = err
		}
	}

	return nil, false, lastErr
}

// backfillCover fills a missing cover from other ISBN-capable sources. The
// primary match often has full text metadata but no cover image (e.g. an
// OpenLibrary record with no cover), and the host has no GetImages fallback for
// this plugin, so the cover must travel on the match itself. We graft a cover
// from the first source that has one for this ISBN.
//
// ponytail: sequential extra fetches, only when the primary match has no
// cover; parallelize if this becomes a latency hotspot.
func (p *Provider) backfillCover(ctx context.Context, match *metadata.Match, q metadata.SearchQuery) {
	if match == nil || strings.TrimSpace(match.CoverURL) != "" {
		return
	}
	isbn := metadata.NormalizeISBN(q.ProviderIDs["isbn"])
	if isbn == "" {
		isbn = metadata.NormalizeISBN(match.ISBN)
	}
	if isbn == "" {
		return
	}
	for _, sourceID := range []string{"googlebooks", "openlibrary", "isbndb"} {
		if sourceID == match.Provider {
			continue // already produced this match and gave no cover
		}
		source := p.byID[sourceID]
		if source == nil {
			continue
		}
		alt, err := source.Fetch(ctx, isbn)
		if err != nil || alt == nil {
			continue
		}
		if cover := strings.TrimSpace(alt.CoverURL); cover != "" {
			match.CoverURL = cover
			return
		}
	}
}

func defaultSources(options Options) []Source {
	userAgent := "silo-plugin-ebook-metadata/0.1"
	sources := []Source{
		NewOpenLibraryClient(userAgent),
		NewGoogleBooksClient(options.GoogleBooksAPIKey, userAgent),
		NewISBNdbClient(options.ISBNdbAPIKey, userAgent),
		NewHardcoverClient(options.HardcoverAPIKey, userAgent),
		NewGoodreadsClient(userAgent),
		NewAmazonClient(userAgent),
		NewAnnasArchiveClient(userAgent),
		NewGutenbergClient(userAgent),
		NewBookBrainzClient(userAgent),
		NewFantasticFictionClient(userAgent),
		NewISFDBClient(userAgent),
		NewLibraryThingClient(userAgent),
		NewInternetArchiveClient(userAgent),
		NewWorldCatClient(userAgent),
		NewDoubanClient(userAgent),
	}
	enabled := enabledSourceSet(options.EnabledSources)
	if len(enabled) == 0 {
		return sources
	}
	filtered := make([]Source, 0, len(sources))
	for _, source := range sources {
		if source == nil {
			continue
		}
		if enabled[strings.TrimSpace(source.ID())] {
			filtered = append(filtered, source)
		}
	}
	return filtered
}

func enabledSourceSet(values []string) map[string]bool {
	out := make(map[string]bool)
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			part = strings.ToLower(strings.TrimSpace(part))
			if part != "" {
				out[part] = true
			}
		}
	}
	return out
}
