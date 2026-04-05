interface SearchBarProps {
    query: string;
    visibleCount: number;
    totalCount: number;
    onQueryChange: (value: string) => void;
}

export const SearchBar = ({
    query,
    visibleCount,
    totalCount,
    onQueryChange,
}: SearchBarProps) => {
    return (
        <section className="search-bar" aria-label="Search maps">
            <h1>DYSON'S MAPS</h1>

            <label className="search-input-wrap" htmlFor="map-search-input">
                <input
                    id="map-search-input"
                    type="search"
                    placeholder="Search maps"
                    value={query}
                    onChange={(event) => onQueryChange(event.target.value)}
                />
            </label>

            <p className="result-count">
                {visibleCount.toLocaleString()} / {totalCount.toLocaleString()} maps
            </p>
        </section>
    );
};
