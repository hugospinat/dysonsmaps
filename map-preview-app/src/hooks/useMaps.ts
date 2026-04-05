import { useEffect, useMemo, useState } from "react";
import type { MapItem } from "../types/MapItem";
import { parseQueueCsv, parseQueueJson } from "../utils/parseCSV";

interface UseMapsResult {
    items: MapItem[];
    loading: boolean;
    error: string | null;
    allTags: string[];
    tagCounts: Map<string, number>;
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
                const sources: Array<{ url: string; kind: "json" | "csv" }> = [
                    { url: `/data/download_queue_web.json?ts=${ts}`, kind: "json" },
                    { url: `/data/download_queue_web.csv?ts=${ts}`, kind: "csv" },
                    { url: `/data/download_queue.csv?ts=${ts}`, kind: "csv" },
                ];

                let parsedItems: MapItem[] = [];
                let loaded = false;
                let lastError = "";

                for (const source of sources) {
                    try {
                        const response = await fetch(source.url);
                        if (!response.ok) {
                            lastError = `${source.url}: ${response.status} ${response.statusText}`;
                            continue;
                        }

                        const text = await response.text();
                        parsedItems = source.kind === "json" ? parseQueueJson(text) : parseQueueCsv(text);
                        loaded = true;
                        break;
                    } catch (error) {
                        const message = error instanceof Error ? error.message : String(error);
                        lastError = `${source.url}: ${message}`;
                    }
                }

                if (!loaded) {
                    throw new Error(`Unable to load queue dataset: ${lastError || "no available source"}`);
                }

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
        tagCounts,
    };
};
