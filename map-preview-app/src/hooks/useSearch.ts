import { useMemo } from "react";
import type { MapItem } from "../types/MapItem";

interface SearchOptions {
    query: string;
    selectedTags: Set<string>;
    excludedTags: Set<string>;
}

const buildTagIndex = (items: MapItem[]): Map<string, number[]> => {
    const index = new Map<string, number[]>();

    items.forEach((item, itemIndex) => {
        item.tags.forEach((tag) => {
            const list = index.get(tag);
            if (list) {
                list.push(itemIndex);
            } else {
                index.set(tag, [itemIndex]);
            }
        });
    });

    return index;
};

const intersectSortedIndices = (left: number[], right: number[]): number[] => {
    let i = 0;
    let j = 0;
    const out: number[] = [];

    while (i < left.length && j < right.length) {
        const a = left[i];
        const b = right[j];
        if (a === b) {
            out.push(a);
            i += 1;
            j += 1;
        } else if (a < b) {
            i += 1;
        } else {
            j += 1;
        }
    }

    return out;
};

export const useSearch = (items: MapItem[], options: SearchOptions): MapItem[] => {
    const normalizedQuery = options.query.trim().toLowerCase();
    const allIndices = useMemo(() => items.map((_, index) => index), [items]);
    const tagIndex = useMemo(() => buildTagIndex(items), [items]);

    return useMemo(() => {
        let candidateIndices: number[] = allIndices;

        if (options.selectedTags.size > 0) {
            const selectedLists: number[][] = [];
            for (const tag of options.selectedTags) {
                const indices = tagIndex.get(tag);
                if (!indices || indices.length === 0) {
                    return [];
                }
                selectedLists.push(indices);
            }

            selectedLists.sort((a, b) => a.length - b.length);
            candidateIndices = selectedLists[0].slice();

            for (let i = 1; i < selectedLists.length; i += 1) {
                candidateIndices = intersectSortedIndices(candidateIndices, selectedLists[i]);
                if (candidateIndices.length === 0) {
                    return [];
                }
            }
        }

        if (options.excludedTags.size > 0 && candidateIndices.length > 0) {
            const excludedUnion = new Set<number>();
            for (const tag of options.excludedTags) {
                const indices = tagIndex.get(tag);
                if (!indices) {
                    continue;
                }
                indices.forEach((idx) => excludedUnion.add(idx));
            }

            if (excludedUnion.size > 0) {
                candidateIndices = candidateIndices.filter((idx) => !excludedUnion.has(idx));
            }
        }

        const candidateItems = candidateIndices.map((idx) => items[idx]);
        if (!normalizedQuery) {
            return candidateItems;
        }

        return candidateItems.filter((item) => item.searchBlob.includes(normalizedQuery));
    }, [allIndices, items, normalizedQuery, options.excludedTags, options.selectedTags, tagIndex]);
};
