import { useEffect, useMemo, useRef, useState } from "react";
import type { MapItem } from "../types/MapItem";
import { MapCard } from "./MapCard";

interface MapGridProps {
    items: MapItem[];
    onOpen: (item: MapItem) => void;
}

export const MapGrid = ({ items, onOpen }: MapGridProps) => {
    const containerRef = useRef<HTMLElement | null>(null);
    const [containerWidth, setContainerWidth] = useState(1200);

    useEffect(() => {
        const container = containerRef.current;
        if (!container) {
            return;
        }

        const observer = new ResizeObserver((entries) => {
            const entry = entries[0];
            if (!entry) {
                return;
            }

            setContainerWidth(Math.floor(entry.contentRect.width));
        });

        observer.observe(container);
        return () => {
            observer.disconnect();
        };
    }, []);

    const columnCount = containerWidth >= 1100 ? 3 : containerWidth >= 700 ? 2 : 1;

    const columnItems = useMemo(() => {
        const columns = Array.from({ length: columnCount }, () => [] as MapItem[]);
        items.forEach((item, index) => {
            columns[index % columnCount].push(item);
        });
        return columns;
    }, [items, columnCount]);

    if (items.length === 0) {
        return (
            <section className="grid-empty-state">
                <h2>No maps found</h2>
                <p>Try a broader search or fewer tag filters.</p>
            </section>
        );
    }

    return (
        <section ref={containerRef} className="map-grid-shell">
            <div
                className="map-grid"
                role="list"
                aria-label="Dyson maps mosaic"
                style={{ gridTemplateColumns: `repeat(${columnCount}, minmax(0, 1fr))` }}
            >
                {columnItems.map((column, columnIndex) => (
                    <div key={`column-${columnIndex}`} className="map-grid-column">
                        {column.map((item) => (
                            <div key={item.id} className="map-grid-item" role="listitem">
                                <MapCard item={item} onOpen={onOpen} />
                            </div>
                        ))}
                    </div>
                ))}
            </div>
        </section>
    );
};
