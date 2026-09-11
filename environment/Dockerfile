# hash:sha256:73d1c8661ba989cf6d680f1e660fbb5348889ee7c9d33ea475414770dd59783e
ARG REGISTRY_HOST
FROM $REGISTRY_HOST/codeocean/code-server-python-extensions-pack:code-server4.129.0python3.12.4codeocean-mcp-serveruvpyreflyetc

ARG DEBIAN_FRONTEND=noninteractive
ARG GIT_ASKPASS
COPY git-ask-pass /

COPY pyproject.toml /tmp/pyproject.toml
RUN uv pip install --system --no-cache -r /tmp/pyproject.toml

# ── LLM Copilot CLI──────────────────────
# Install Node.js (needed for copilot CLI)
RUN conda install -y nodejs -c conda-forge \
    && conda clean -ya

# Install GitHub Copilot CLI globally (no auth needed for install)
RUN npm install -g @github/copilot
#──────────────────────