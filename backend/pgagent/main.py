"""Entry point for pgAgent."""

import logging
import signal
import sys

from pgagent.config import Settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("pgagent")


def main() -> None:
    """Start pgAgent."""
    settings = Settings()  # type: ignore[call-arg]
    logger.info("pgAgent starting on %s:%s", settings.api_host, settings.api_port)

    # Lazy imports to keep startup fast and allow circular-free loading
    from pgagent.agent import Agent
    from pgagent.api.app import create_app

    agent = Agent(settings)
    app = create_app(agent)

    def _shutdown(signum: int, frame: object) -> None:
        logger.info("Received signal %s, shutting down…", signum)
        agent.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    agent.start()

    import uvicorn

    uvicorn.run(app, host=settings.api_host, port=settings.api_port, log_level="info")


if __name__ == "__main__":
    main()
