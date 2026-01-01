import sys
import collections
import statistics
import os

# Try to import matplotlib; handle case where it's missing
try:
    import matplotlib

    matplotlib.use("Agg")  # Crucial for running on servers/pipelines without a monitor
    import matplotlib.pyplot as plt

    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    print(
        "⚠️  Matplotlib not found. Visual plots will be skipped. (pip install matplotlib)"
    )


def print_text_histogram(values, buckets=10):
    """Generates a text-based histogram for the console logs."""
    if not values:
        return
    min_v, max_v = min(values), max(values)
    if min_v == max_v:
        return

    bucket_size = (max_v - min_v) / buckets
    counts = [0] * buckets
    for v in values:
        idx = int((v - min_v) / bucket_size)
        if idx >= buckets:
            idx = buckets - 1
        counts[idx] += 1

    max_count = max(counts)
    scale = 40.0 / max_count if max_count > 0 else 1

    print(f"\n  Console Histogram:")
    for i in range(buckets):
        low = min_v + (i * bucket_size)
        high = min_v + ((i + 1) * bucket_size)
        bar = "#" * int(counts[i] * scale)
        print(f"  [{int(low):>6} - {int(high):<6}]: {bar} ({counts[i]})")


def save_visual_plot(token_counts, filepath):
    """Generates and saves a PNG image of the distribution."""
    if not HAS_MATPLOTLIB:
        return

    output_filename = filepath + "_dist.png"
    print(f"🎨 Generating plot: {output_filename}...")

    plt.figure(figsize=(10, 6))

    # Histogram of token counts
    plt.hist(
        token_counts,
        bins=50,
        color="skyblue",
        edgecolor="black",
        alpha=0.7,
        label="Sample Count",
    )

    # Add labels and title
    plt.title(f"Token Count Distribution: {os.path.basename(filepath)}", fontsize=14)
    plt.xlabel("Number of Tokens per Sample", fontsize=12)
    plt.ylabel("Frequency", fontsize=12)
    plt.grid(axis="y", alpha=0.5)
    plt.legend()

    # Save output
    plt.savefig(output_filename)
    plt.close()
    print(f"✅ Plot saved successfully.")


def analyze(filepath):
    print(f"--- 📊 Analyzing {filepath} ---")

    token_counts = []
    char_counter = collections.Counter()
    total_chars = 0
    current_tokens = 0
    sample_count = 0

    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                stripped = line.strip()
                if stripped == "<eos>":
                    if current_tokens > 0:
                        token_counts.append(current_tokens)
                        sample_count += 1
                    current_tokens = 0
                    continue

                # Update stats
                current_tokens += len(stripped.split())
                char_counter.update(line)
                total_chars += len(line)

    except FileNotFoundError:
        print("❌ Error: File not found.")
        sys.exit(1)

    if not token_counts:
        print("❌ No samples found.")
        sys.exit(1)

    # --- REPORTING ---
    avg_tokens = statistics.mean(token_counts)
    max_tokens = max(token_counts)
    min_tokens = min(token_counts)

    print(f"\n✅ Samples: {sample_count:,} | Tokens: {sum(token_counts):,}")
    print(f"📏 Avg: {avg_tokens:.1f} | Min: {min_tokens} | Max: {max_tokens}")

    # 1. Console Histogram (Quick check in logs)
    print_text_histogram(token_counts)

    # 2. Matplotlib Image (The curve you asked for)
    save_visual_plot(token_counts, filepath)

    # 3. Quality Checks
    non_ascii_count = sum(
        count for char, count in char_counter.items() if ord(char) > 127
    )
    non_ascii_ratio = non_ascii_count / total_chars if total_chars > 0 else 0
    print(f"\n🌍 Non-ASCII Ratio: {non_ascii_ratio:.2%}")

    if non_ascii_ratio > 0.2:
        print("⚠️  WARNING: High non-ASCII content (Check for binary/foreign text)")

    print("--- Analysis Complete ---")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python analyze_dataset.py <filepath>")
        sys.exit(1)

    analyze(sys.argv[1])
