import Papa from "papaparse";
import type { MapCsvRow, MapItem } from "../types/MapItem";

interface QueueJsonPayload {
    items?: MapCsvRow[];
}

const parseNumber = (value: string | number | undefined): number | null => {
    if (value === undefined || value === null || value === "") {
        return null;
    }

    const num = Number(value);
    return Number.isFinite(num) ? num : null;
};

const parseBoolean = (value: string | number | undefined): boolean => {
    if (value === undefined || value === null) {
        return false;
    }

    const normalized = String(value).trim().toLowerCase();
    return normalized === "1" || normalized === "true" || normalized === "yes";
};

const toTitleCase = (value: string): string => {
    return value
        .split(" ")
        .filter((part) => part.length > 0)
        .map((part) => part[0].toUpperCase() + part.slice(1).toLowerCase())
        .join(" ");
};

const getCanonicalTag = (rawTag: string): string => {
    const normalized = rawTag
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
        .replace(/[‘’'“”"]/g, "")
        .replace(/&/g, " and ")
        .replace(/[^a-z0-9]+/g, " ")
        .trim()
        .replace(/\s+/g, " ");

    if (!normalized) {
        return "";
    }

    if (normalized === "map" || normalized === "maps") {
        return "Maps";
    }

    if (normalized === "rpg" || normalized === "rpgs") {
        return "RPG";
    }

    if (normalized === "osr") {
        return "OSR";
    }

    if (normalized === "ose") {
        return "OSE";
    }

    if (normalized === "dcc") {
        return "DCC";
    }

    if (normalized === "dnd" || normalized === "dd" || normalized === "d and d") {
        return "Dungeons & Dragons";
    }

    if (
        normalized === "black and white"
        || normalized === "black white"
        || normalized === "bw"
        || normalized === "b w"
        || normalized === "grayscale"
        || normalized === "greyscale"
    ) {
        return "Black & White";
    }

    if (/^dung\w*\s+and\s+drag\w*$/.test(normalized) || /^dung\w*\s+drag\w*$/.test(normalized)) {
        return "Dungeons & Dragons";
    }

    return toTitleCase(normalized);
};

const splitTags = (tagsRaw: string | string[] | undefined): string[] => {
    if (!tagsRaw) {
        return [];
    }

    const parts = Array.isArray(tagsRaw) ? tagsRaw : tagsRaw.split("|");

    const deduped = new Set<string>();
    for (const part of parts) {
        const cleaned = getCanonicalTag(part.trim());
        if (cleaned.length > 0) {
            deduped.add(cleaned);
        }
    }

    return Array.from(deduped);
};

const getSourceStem = (sourceFile: string): string => {
    const normalized = sourceFile.replace(/\\/g, "/");
    const file = normalized.split("/").pop() || "unknown-source.html";
    return file.replace(/\.[^.]+$/, "");
};

const encodePathSegment = (segment: string): string => encodeURIComponent(segment);

const buildLocalPath = (sourceStem: string, fileName: string): string => {
    return `/data/map_assets/${encodePathSegment(sourceStem)}/${encodePathSegment(fileName)}`;
};

const normalizeWebPath = (rawPath: string): string => {
    const normalized = rawPath.trim().replace(/\\/g, "/");
    if (!normalized) {
        return "";
    }

    if (normalized.startsWith("http://") || normalized.startsWith("https://")) {
        return normalized;
    }

    if (normalized.startsWith("/")) {
        return normalized;
    }

    const downloadsIdx = normalized.toLowerCase().indexOf("downloads_web/");
    if (downloadsIdx >= 0) {
        return `/${normalized.slice(downloadsIdx)}`;
    }

    return `/${normalized}`;
};

const extractPublishedDate = (canonicalUrl: string): string | null => {
    const match = canonicalUrl.match(/\/(\d{4})\/(\d{2})\/(\d{2})\//);
    if (!match) {
        return null;
    }

    return `${match[1]}-${match[2]}-${match[3]}`;
};

const toMapItem = (row: MapCsvRow, index: number): MapItem | null => {
    if ((row.asset_type || "").trim().toLowerCase() !== "image") {
        return null;
    }

    const sourceFile = (row.source_file || "").trim();
    const fileName = (row.file_name || "").trim();
    const remoteImageSrc = (row.url || "").trim();

    if (!sourceFile || !fileName || !remoteImageSrc) {
        return null;
    }

    const sourceStem = getSourceStem(sourceFile);
    const title = (row.title || "Untitled map").trim();
    const tags = splitTags(row.tags);
    const fileStem = (row.file_stem || fileName.replace(/\.[^.]+$/, "")).trim();
    const fileExt = (row.file_ext || fileName.split(".").pop() || "").trim();
    const canonicalUrl = (row.canonical_url || "").trim();
    const publishedDate = extractPublishedDate(canonicalUrl);
    const candidateRank = parseNumber(row.candidate_rank);
    const candidateScore = parseNumber(row.candidate_score);
    const isBestCandidate = parseBoolean(row.is_best_candidate);
    const isBw = parseBoolean(row.is_bw);
    const previewImageSrc = normalizeWebPath(String(row.preview_path || ""));
    const originalImageSrc = normalizeWebPath(String(row.original_path || ""));
    const previewWidth = parseNumber(row.preview_width);
    const previewHeight = parseNumber(row.preview_height);
    const previewSizeKb = parseNumber(row.preview_size_kb);

    if (isBw) {
        tags.push("Black & White");
    }

    const dedupedTags = Array.from(new Set(tags));
    const tagSet = new Set(dedupedTags);

    const searchBlob = [title, fileName, sourceStem, publishedDate || "", dedupedTags.join(" ")]
        .join(" ")
        .toLowerCase();

    return {
        id: `${sourceStem}-${fileName}-${index}`,
        title,
        publishedDate,
        isBw,
        tags: dedupedTags,
        tagSet,
        searchBlob,
        sourceFile,
        sourceStem,
        fileName,
        fileStem,
        fileExt,
        canonicalUrl,
        candidateRank,
        candidateScore,
        isBestCandidate,
        previewImageSrc,
        previewWidth,
        previewHeight,
        previewSizeKb,
        localImageSrc: originalImageSrc || buildLocalPath(sourceStem, fileName),
        remoteImageSrc,
    };
};

export const parseQueueCsv = (csvText: string): MapItem[] => {
    const parsed = Papa.parse<MapCsvRow>(csvText, {
        header: true,
        skipEmptyLines: true,
    });

    if (parsed.errors.length > 0) {
        const first = parsed.errors[0];
        throw new Error(`CSV parse error at row ${first.row}: ${first.message}`);
    }

    return parseQueueRows(parsed.data);
};

export const parseQueueRows = (rows: MapCsvRow[]): MapItem[] => {
    const items: MapItem[] = [];
    rows.forEach((row, index) => {
        const item = toMapItem(row, index);
        if (item) {
            items.push(item);
        }
    });

    items.sort((a, b) => {
        const bestDiff = Number(b.isBestCandidate) - Number(a.isBestCandidate);
        if (bestDiff !== 0) {
            return bestDiff;
        }

        const scoreA = a.candidateScore ?? -9999;
        const scoreB = b.candidateScore ?? -9999;
        if (scoreB !== scoreA) {
            return scoreB - scoreA;
        }

        return a.title.localeCompare(b.title);
    });

    return items;
};

export const parseQueueJson = (jsonText: string): MapItem[] => {
    let parsed: unknown;
    try {
        parsed = JSON.parse(jsonText);
    } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        throw new Error(`JSON parse error: ${message}`);
    }

    let rows: MapCsvRow[] = [];
    if (Array.isArray(parsed)) {
        rows = parsed as MapCsvRow[];
    } else if (parsed && typeof parsed === "object") {
        rows = ((parsed as QueueJsonPayload).items || []) as MapCsvRow[];
    }

    if (!Array.isArray(rows)) {
        throw new Error("Invalid queue JSON format: expected an array or an object with an items array.");
    }

    return parseQueueRows(rows);
};
