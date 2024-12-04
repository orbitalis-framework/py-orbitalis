

class PluginConfiguration:

    def __init__(self, max_supported_cores: int | None, supported_cores: list[str] | None = None):

        if max_supported_cores is not None and max_supported_cores <= 0:
            raise ValueError("plugin must support at least 1 core (use None to indicate no limits)")

        self.max_supported_cores = max_supported_cores
        self.supported_cores = supported_cores
