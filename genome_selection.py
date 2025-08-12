import re, requests, pandas as pd, streamlit as st

MANIFEST_URL = "https://raw.githubusercontent.com/ewels/AWS-iGenomes/master/ngi-igenomes_file_manifest.txt"


# a simple allow-list of plant species names seen in iGenomes; expand anytime
PLANT_SPECIES_KEYS = {
    "Arabidopsis_thaliana",
    "Oryza_sativa_japonica",
    "Zea_mays",
    "Sorghum_bicolor",
    "Glycine_max",
    "Solanum_lycopersicum",
    "Brachypodium_distachyon",
    "Populus_trichocarpa",
    "Physcomitrella_patens",
    "Setaria_italica",
}


@st.cache_data(ttl=24 * 60 * 60)
def load_igenomes():
    # Parse S3 paths like:
    # s3://ngi-igenomes/igenomes/<Genome>/<Source>/<Build>/<TopDir>/...
    txt = requests.get(MANIFEST_URL, timeout=30).text
    rows = []
    for line in txt.splitlines():
        m = re.match(
            r"s3://ngi-igenomes/igenomes/([^/]+)/([^/]+)/([^/]+)/([^/]+)/(.+)$", line
        )
        if m:
            genome, source, build, topdir, rest = m.groups()
            rows.append((genome, source, build, topdir, rest))
    df = pd.DataFrame(rows, columns=["genome", "source", "build", "topdir", "path"])
    # keep only plants (by species folder name)
    df = df[df["genome"].isin(PLANT_SPECIES_KEYS)]
    return df


def select_reference_genome():
    df = load_igenomes()

    # Pretty display names
    def pretty_label(genome, source, build):
        return f"{genome.replace('_',' ')} ({build}, {source})"

    # Dropdown
    choice = st.selectbox(
        "Select plant reference (FASTA + annotation will be fetched automatically)",
        [pretty_label(*x) for x in df.to_records(index=False)],
        index=None,
        placeholder="Type to search...",
    )

    if not choice:
        return None
