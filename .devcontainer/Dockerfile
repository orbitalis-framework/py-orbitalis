FROM mcr.microsoft.com/devcontainers/python:3.12

# === Install Mosquitto ===
RUN apt-get update && apt-get install -y mosquitto mosquitto-clients

# === Setup Workspace ===
WORKDIR /workspace

COPY requirements.txt /workspace/

RUN pip install --upgrade pip && \
    pip install -r /workspace/requirements.txt

USER vscode

CMD ["bash"]