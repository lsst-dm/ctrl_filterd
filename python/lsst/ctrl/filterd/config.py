# This file is part of ctrl_filterd
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import yaml
from pydantic import BaseModel, Field


class Config(BaseModel):
    brokers: list[str] = Field(min_length=1)
    consumer_client_id: str = "filterd_consumer"
    producer_client_id: str = "filterd_producer"
    group_id: str = "filterd_groupid"
    num_messages: int = Field(default=50, ge=1, le=1_000_000)
    timeout: float = Field(default=1.0, gt=0)
    topic: str
    rse_topics: list[str] = Field(min_length=1)

    @classmethod
    def load(cls, config_file: str) -> "Config":
        """Load and validate a configuration file.

        Parameters
        ----------
        config_file : `str`
            Path to the YAML configuration file.

        Returns
        -------
        config : `Config`
            The validated configuration.

        Raises
        ------
        OSError
            Raised if the file cannot be opened.
        ValueError
            Raised if the file is not valid YAML or does not contain a
            mapping.
        pydantic.ValidationError
            Raised if the contents fail validation (a subclass of
            `ValueError`).
        """
        try:
            with open(config_file) as file:
                config_dict = yaml.safe_load(file)
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing {config_file}: {e}") from e
        if not isinstance(config_dict, dict):
            raise ValueError(f"{config_file} must be a valid YAML mapping; got: {type(config_dict).__name__}")
        return cls.model_validate(config_dict)
