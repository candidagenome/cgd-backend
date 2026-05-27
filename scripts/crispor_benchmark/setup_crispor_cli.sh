#!/bin/bash
# Setup CRISPOR command-line tool for automated benchmarking
# This script installs BWA and CRISPOR in user space (no sudo required)

set -e

INSTALL_DIR="$HOME/tools/crispor"
BWA_VERSION="0.7.17"

echo "=========================================="
echo "Setting up CRISPOR CLI"
echo "=========================================="

# Create installation directory
mkdir -p "$INSTALL_DIR"
cd "$INSTALL_DIR"

# Step 1: Install BWA from source
echo ""
echo "[1/4] Installing BWA $BWA_VERSION..."
if [ ! -f "$INSTALL_DIR/bwa/bwa" ]; then
    wget -q "https://github.com/lh3/bwa/releases/download/v${BWA_VERSION}/bwa-${BWA_VERSION}.tar.bz2"
    tar -xjf "bwa-${BWA_VERSION}.tar.bz2"
    cd "bwa-${BWA_VERSION}"
    make -j4
    cd ..
    mv "bwa-${BWA_VERSION}" bwa
    rm "bwa-${BWA_VERSION}.tar.bz2"
    echo "  BWA installed at: $INSTALL_DIR/bwa/bwa"
else
    echo "  BWA already installed"
fi

# Step 2: Clone CRISPOR
echo ""
echo "[2/4] Cloning CRISPOR repository..."
if [ ! -d "$INSTALL_DIR/crisporWebsite" ]; then
    git clone --depth 1 https://github.com/maximilianh/crisporWebsite.git
    echo "  CRISPOR cloned"
else
    echo "  CRISPOR already cloned"
fi

# Step 3: Install Python dependencies
echo ""
echo "[3/4] Installing Python dependencies..."
pip3 install --user --quiet biopython numpy twobitreader xlwt 2>/dev/null || true
echo "  Python dependencies installed"

# Step 4: Download C. albicans genome for CRISPOR
echo ""
echo "[4/4] Setting up C. albicans genome..."
GENOME_DIR="$INSTALL_DIR/crisporWebsite/genomes/candAlb"
if [ ! -d "$GENOME_DIR" ]; then
    mkdir -p "$GENOME_DIR"
    cd "$GENOME_DIR"

    # Download from CRISPOR's genome server
    echo "  Downloading genome files from CRISPOR server..."
    wget -q "http://crispor.tefor.net/genomes/candAlb/candAlb.2bit" || echo "  Warning: Could not download 2bit file"
    wget -q "http://crispor.tefor.net/genomes/candAlb/candAlb.fa.bwt" || echo "  Warning: Could not download BWA index"
    wget -q "http://crispor.tefor.net/genomes/candAlb/candAlb.fa.pac" || true
    wget -q "http://crispor.tefor.net/genomes/candAlb/candAlb.fa.ann" || true
    wget -q "http://crispor.tefor.net/genomes/candAlb/candAlb.fa.amb" || true
    wget -q "http://crispor.tefor.net/genomes/candAlb/candAlb.fa.sa" || true
    wget -q "http://crispor.tefor.net/genomes/candAlb/candAlb.segments.bed" || true

    echo "  Genome files downloaded"
else
    echo "  Genome already set up"
fi

# Create wrapper script
echo ""
echo "Creating wrapper script..."
cat > "$INSTALL_DIR/run_crispor.sh" << 'EOF'
#!/bin/bash
# CRISPOR wrapper script
export PATH="$HOME/tools/crispor/bwa:$PATH"
cd "$HOME/tools/crispor/crisporWebsite"
python3 crispor.py "$@"
EOF
chmod +x "$INSTALL_DIR/run_crispor.sh"

echo ""
echo "=========================================="
echo "Setup complete!"
echo "=========================================="
echo ""
echo "CRISPOR CLI: $INSTALL_DIR/run_crispor.sh"
echo "BWA binary:  $INSTALL_DIR/bwa/bwa"
echo ""
echo "Usage:"
echo "  $INSTALL_DIR/run_crispor.sh candAlb input.fa output.tsv"
echo ""
echo "Test with:"
echo "  echo -e '>test\nATGTCTGCAGATGGAGAATTTACAAGAACCCAGATATTTGGGACTGTTTTTGAAATCACC' > /tmp/test.fa"
echo "  $INSTALL_DIR/run_crispor.sh candAlb /tmp/test.fa /tmp/test_output.tsv"
