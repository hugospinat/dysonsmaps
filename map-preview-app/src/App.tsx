import { useCallback, useEffect, useMemo, useState, useTransition } from "react";
import { MapGrid } from "./components/MapGrid";
import { PreviewModal } from "./components/PreviewModal";
import { SearchBar } from "./components/SearchBar";
import { TagFilter } from "./components/TagFilter";
import { useMaps } from "./hooks/useMaps";
import { useSearch } from "./hooks/useSearch";
import type { MapItem } from "./types/MapItem";

const App = () => {
    const DEFAULT_BW_TAG = "Black & White";
    const { items, loading, error, allTags, tagCounts } = useMaps();
    const [query, setQuery] = useState("");
    const [selectedTags, setSelectedTags] = useState<Set<string>>(new Set());
    const [excludedTags, setExcludedTags] = useState<Set<string>>(new Set());
    const [activeItemId, setActiveItemId] = useState<string | null>(null);
    const [bwDefaultApplied, setBwDefaultApplied] = useState(false);
    const [, startTransition] = useTransition();

    const filteredItems = useSearch(items, {
        query,
        selectedTags,
        excludedTags,
    });

    const activeIndex = useMemo(() => {
        if (!activeItemId) {
            return -1;
        }
        return filteredItems.findIndex((item) => item.id === activeItemId);
    }, [activeItemId, filteredItems]);

    const activeItem: MapItem | null = activeIndex >= 0 ? filteredItems[activeIndex] : null;

    useEffect(() => {
        if (bwDefaultApplied || loading || error || allTags.length === 0) {
            return;
        }

        setBwDefaultApplied(true);
        if (allTags.includes(DEFAULT_BW_TAG)) {
            setSelectedTags(new Set([DEFAULT_BW_TAG]));
            setExcludedTags(new Set());
        }
    }, [allTags, bwDefaultApplied, loading, error]);

    const toggleTag = useCallback((tag: string) => {
        startTransition(() => {
            setSelectedTags((prev) => {
                const next = new Set(prev);
                if (next.has(tag)) {
                    next.delete(tag);
                } else {
                    next.add(tag);
                }
                return next;
            });

            setExcludedTags((prev) => {
                if (!prev.has(tag)) {
                    return prev;
                }
                const next = new Set(prev);
                next.delete(tag);
                return next;
            });
        });
    }, [startTransition]);

    const toggleExcludeTag = useCallback((tag: string) => {
        startTransition(() => {
            setExcludedTags((prev) => {
                const next = new Set(prev);
                if (next.has(tag)) {
                    next.delete(tag);
                } else {
                    next.add(tag);
                }
                return next;
            });

            setSelectedTags((prev) => {
                if (!prev.has(tag)) {
                    return prev;
                }
                const next = new Set(prev);
                next.delete(tag);
                return next;
            });
        });
    }, [startTransition]);

    const clearTags = useCallback(() => {
        startTransition(() => {
            setSelectedTags(new Set());
            setExcludedTags(new Set());
        });
    }, [startTransition]);

    const openItem = useCallback((item: MapItem) => {
        setActiveItemId(item.id);
    }, []);

    const closeModal = () => {
        setActiveItemId(null);
    };

    const goNext = () => {
        if (filteredItems.length === 0 || activeIndex < 0) {
            return;
        }

        const nextIndex = (activeIndex + 1) % filteredItems.length;
        setActiveItemId(filteredItems[nextIndex].id);
    };

    const goPrevious = () => {
        if (filteredItems.length === 0 || activeIndex < 0) {
            return;
        }

        const prevIndex = (activeIndex - 1 + filteredItems.length) % filteredItems.length;
        setActiveItemId(filteredItems[prevIndex].id);
    };

    return (
        <div className="app-shell">
            <aside className="left-rail">
                <SearchBar
                    query={query}
                    visibleCount={filteredItems.length}
                    totalCount={items.length}
                    onQueryChange={setQuery}
                />

                <TagFilter
                    tags={allTags}
                    selectedTags={selectedTags}
                    excludedTags={excludedTags}
                    tagCounts={tagCounts}
                    onToggleTag={toggleTag}
                    onCycleSelectedTag={toggleExcludeTag}
                    onClear={clearTags}
                />
            </aside>

            <main className="main-panel">
                {loading ? <p className="state-text">Loading maps...</p> : null}
                {error ? <p className="state-text error">{error}</p> : null}
                {!loading && !error ? <MapGrid items={filteredItems} onOpen={openItem} /> : null}
            </main>

            {activeItem ? (
                <PreviewModal
                    item={activeItem}
                    index={activeIndex}
                    total={filteredItems.length}
                    onClose={closeModal}
                    onNext={goNext}
                    onPrevious={goPrevious}
                />
            ) : null}
        </div>
    );
};

export default App;
