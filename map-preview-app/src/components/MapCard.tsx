import { memo, useEffect, useMemo, useState } from "react";
import type { MapItem } from "../types/MapItem";

interface MapCardProps {
    item: MapItem;
    onOpen: (item: MapItem) => void;
}

const MapCardComponent = ({ item, onOpen }: MapCardProps) => {
    const sourceChain = useMemo(() => {
        const sources = [item.previewImageSrc, item.localImageSrc, item.remoteImageSrc]
            .filter((source) => typeof source === "string" && source.trim().length > 0);
        return Array.from(new Set(sources));
    }, [item.previewImageSrc, item.localImageSrc, item.remoteImageSrc]);

    const [sourceIndex, setSourceIndex] = useState(0);
    const src = sourceChain[sourceIndex] || item.remoteImageSrc;

    useEffect(() => {
        setSourceIndex(0);
    }, [item.id, sourceChain]);

    const handleError = () => {
        setSourceIndex((prev) => {
            if (prev >= sourceChain.length - 1) {
                return prev;
            }
            return prev + 1;
        });
    };

    const handleOpen = () => {
        onOpen(item);
    };

    return (
        <article
            className="map-card"
            onClick={handleOpen}
            role="button"
            tabIndex={0}
            onKeyDown={(event) => {
                if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    handleOpen();
                }
            }}
        >
            <div className="map-card-image-wrap">
                <img
                    className="map-card-image"
                    src={src}
                    alt={item.title}
                    loading="lazy"
                    onError={handleError}
                    width={item.previewWidth || undefined}
                    height={item.previewHeight || undefined}
                />

                <div className="map-card-hover-info" aria-hidden="true">
                    <p className="map-card-info-line">
                        <strong>Name:</strong> {item.title}
                    </p>
                    <p className="map-card-info-line map-card-info-tags">
                        <strong>Tags:</strong> {item.tags.length > 0 ? item.tags.join(", ") : "No tags"}
                    </p>
                </div>
            </div>
        </article>
    );
};

export const MapCard = memo(MapCardComponent, (prev, next) => {
    return prev.item === next.item && prev.onOpen === next.onOpen;
});
