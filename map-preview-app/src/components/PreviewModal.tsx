import { useEffect, useState } from "react";
import type { MapItem } from "../types/MapItem";

interface PreviewModalProps {
    item: MapItem;
    index: number;
    total: number;
    onClose: () => void;
    onNext: () => void;
    onPrevious: () => void;
}

export const PreviewModal = ({
    item,
    index,
    total,
    onClose,
    onNext,
    onPrevious,
}: PreviewModalProps) => {
    const [src, setSrc] = useState(item.localImageSrc || item.remoteImageSrc);
    const [usingFallback, setUsingFallback] = useState(false);
    const downloadSrc = item.localImageSrc || item.remoteImageSrc;

    useEffect(() => {
        setSrc(item.localImageSrc || item.remoteImageSrc);
        setUsingFallback(false);
    }, [item.id, item.localImageSrc, item.remoteImageSrc]);

    useEffect(() => {
        const onKeyDown = (event: KeyboardEvent) => {
            if (event.key === "Escape") {
                onClose();
            } else if (event.key === "ArrowRight") {
                onNext();
            } else if (event.key === "ArrowLeft") {
                onPrevious();
            }
        };

        window.addEventListener("keydown", onKeyDown);
        return () => {
            window.removeEventListener("keydown", onKeyDown);
        };
    }, [onClose, onNext, onPrevious]);

    const handleError = () => {
        if (!usingFallback && item.remoteImageSrc) {
            setSrc(item.remoteImageSrc);
            setUsingFallback(true);
        }
    };

    return (
        <div className="modal-backdrop" onClick={onClose}>
            <div className="modal-shell" onClick={(event) => event.stopPropagation()}>
                <button type="button" className="modal-close" onClick={onClose}>
                    Close
                </button>

                <div className="modal-toolbar">
                    <button type="button" onClick={onPrevious} className="nav-button">
                        Previous
                    </button>
                    <p>
                        {index + 1} / {total}
                    </p>
                    <button type="button" onClick={onNext} className="nav-button">
                        Next
                    </button>
                </div>

                <img className="modal-image" src={src} alt={item.title} onError={handleError} />

                <div className="modal-meta">
                    <h2>{item.title}</h2>
                    <p>{item.fileName}</p>
                    <div className="modal-links">
                        <a href={src} target="_blank" rel="noreferrer">
                            Open full image
                        </a>
                        <a href={downloadSrc} download={item.fileName}>
                            Download image
                        </a>
                        {item.canonicalUrl ? (
                            <a href={item.canonicalUrl} target="_blank" rel="noreferrer">
                                Source page
                            </a>
                        ) : null}
                    </div>
                </div>
            </div>
        </div>
    );
};
