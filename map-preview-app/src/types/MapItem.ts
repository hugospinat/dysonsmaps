export interface MapCsvRow {
    asset_type?: string;
    url?: string;
    file_name?: string;
    file_stem?: string;
    file_ext?: string;
    source_file?: string;
    canonical_url?: string;
    title?: string;
    tags?: string | string[];
    candidate_rank?: string | number;
    candidate_score?: string | number;
    is_best_candidate?: string | number;
    is_bw?: string | number;
    preview_path?: string;
    original_path?: string;
    preview_width?: string | number;
    preview_height?: string | number;
    preview_size_kb?: string | number;
}

export interface MapItem {
    id: string;
    title: string;
    publishedDate: string | null;
    isBw: boolean;
    tags: string[];
    tagSet: Set<string>;
    searchBlob: string;
    sourceFile: string;
    sourceStem: string;
    fileName: string;
    fileStem: string;
    fileExt: string;
    canonicalUrl: string;
    candidateRank: number | null;
    candidateScore: number | null;
    isBestCandidate: boolean;
    previewImageSrc: string;
    previewWidth: number | null;
    previewHeight: number | null;
    previewSizeKb: number | null;
    localImageSrc: string;
    remoteImageSrc: string;
}
