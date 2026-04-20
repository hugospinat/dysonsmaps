import { useEffect, useMemo, useState } from "react";
import type { MapItem } from "../types/MapItem";
import { parseQueueJson } from "../utils/parseCSV";

interface UseMapsResult {
    items: MapItem[];
    loading: boolean;
    error: string | null;
    allTags: string[];
}

export const useMaps = (): UseMapsResult => {
    const [items, setItems] = useState<MapItem[]>([]);
    const [loading, setLoading] = useState<boolean>(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        let cancelled = false;

        const load = async () => {
            setLoading(true);
            setError(null);

            try {
                const ts = Date.now();
                const url = `/data/download_queue_web.json?ts=${ts}`;
                const response = await fetch(url);

                if (!response.ok) {
                    throw new Error(`Unable to load queue dataset: ${url}: ${response.status} ${response.statusText}`);
                }

                const text = await response.text();
                const parsedItems = parseQueueJson(text);

                if (!cancelled) {
                    setItems(parsedItems);
                }
            } catch (err) {
                const message = err instanceof Error ? err.message : String(err);
                if (!cancelled) {
                    setError(message);
                    setItems([]);
                }
            } finally {
                if (!cancelled) {
                    setLoading(false);
                }
            }
        };

        void load();

        return () => {
            cancelled = true;
        };
    }, []);

    const tagCounts = useMemo(() => {
        const counts = new Map<string, number>();
        for (const item of items) {
            for (const tag of item.tags) {
                counts.set(tag, (counts.get(tag) || 0) + 1);
            }
        }
        return counts;
    }, [items]);

    const allTags = useMemo(() => {
        return Array.from(tagCounts.entries())
            .sort((a, b) => {
                if (b[1] !== a[1]) {
                    return b[1] - a[1];
                }
                return a[0].localeCompare(b[0]);
            })
            .map((entry) => entry[0]);
    }, [tagCounts]);

    return {
        items,
        loading,
        error,
        allTags,
    };
};
