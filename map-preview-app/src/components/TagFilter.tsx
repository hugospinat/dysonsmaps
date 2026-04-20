import { useMemo, useState } from "react";

interface TagFilterProps {
    tags: string[];
    selectedTags: Set<string>;
    tagCounts: Map<string, number>;
    onToggleTag: (tag: string) => void;
    onClear: () => void;
}

export const TagFilter = ({
    tags,
    selectedTags,
    tagCounts,
    onToggleTag,
    onClear,
}: TagFilterProps) => {
    const [tagQuery, setTagQuery] = useState("");

    const filteredTags = useMemo(() => {
        const q = tagQuery.trim().toLowerCase();
        if (!q) {
            return tags;
        }
        return tags.filter((tag) => tag.toLowerCase().includes(q));
    }, [tags, tagQuery]);

    return (
        <section className="tag-filter" aria-label="Tag filters">
            <div className="tag-filter-header">
                <h2>Tags</h2>
                <button type="button" onClick={onClear} className="ghost-button">
                    Clear
                </button>
            </div>

            <label className="tag-search-wrap" htmlFor="tag-search-input">
                <input
                    id="tag-search-input"
                    type="search"
                    placeholder="Search tags"
                    value={tagQuery}
                    onChange={(event) => setTagQuery(event.target.value)}
                />
            </label>

            <div className="tag-filter-list" role="listbox" aria-label="Map tags">
                {filteredTags.map((tag) => {
                    const selected = selectedTags.has(tag);
                    const count = tagCounts.get(tag) || 0;
                    return (
                        <button
                            key={tag}
                            type="button"
                            className={`tag-row ${selected ? "selected" : ""}`}
                            onClick={() => onToggleTag(tag)}
                        >
                            <span className="tag-label">{tag}</span>
                            <span className="count">{count}</span>
                        </button>
                    );
                })}
            </div>

            <div className="selected-tags-panel" aria-label="Selected tags summary">
                <p className="selected-tags-title">Selected tags</p>
                <div className="selected-tags-list">
                    {selectedTags.size === 0 ? <span className="selected-tags-empty">None</span> : null}
                    {Array.from(selectedTags).map((tag) => (
                        <button
                            key={`include-${tag}`}
                            type="button"
                            className="selected-tag-chip"
                            onClick={() => onToggleTag(tag)}
                            title={`Remove ${tag}`}
                        >
                            {tag}
                        </button>
                    ))}
                </div>
            </div>
        </section>
    );
};
