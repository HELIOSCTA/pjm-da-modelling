from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from backend.utils import logging_utils

from backend.scrapes.ice_python import utils

API_SCRAPE_NAME = "install_ice_python"

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)


def check_icepython_installation() -> str | None:
    try:
        module = utils.get_icepython_module()
    except ModuleNotFoundError:
        return None
    return getattr(module, "__file__", None)


def main(
    ice_bin_path: Path = utils.DEFAULT_ICE_XL_BIN_PATH,
    wheel_name: str = utils.DEFAULT_ICE_WHEEL,
) -> bool:
    try:
        logger.header(API_SCRAPE_NAME)

        installed_path = check_icepython_installation()
        if installed_path:
            logger.info(f"Existing icepython installation: {installed_path}")
        else:
            logger.info("icepython is not currently installed.")

        wheel_path = ice_bin_path / wheel_name
        if not ice_bin_path.exists():
            raise FileNotFoundError(f"ICE XL bin directory not found: {ice_bin_path}")
        if not wheel_path.exists():
            raise FileNotFoundError(f"ICE XL wheel not found: {wheel_path}")

        command = [sys.executable, "-m", "pip", "install", str(wheel_path), "--force-reinstall"]
        logger.info(f"Installing wheel: {wheel_path}")
        result = subprocess.run(command, capture_output=True, text=True, check=False)

        if result.stdout:
            logger.info(result.stdout.strip())
        if result.returncode != 0:
            if result.stderr:
                logger.error(result.stderr.strip())
            raise RuntimeError(f"pip install failed with exit code {result.returncode}")

        installed_path = check_icepython_installation()
        logger.success(f"icepython installed successfully: {installed_path}")
        return True

    finally:
        logging_utils.close_logging()


if __name__ == "__main__":
    main()

