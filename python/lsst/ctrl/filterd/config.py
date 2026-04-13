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
    brokers: list[str]
    consumer_client_id: str = Field(default_factory=lambda: "filterd_consumer")
    producer_client_id: str = Field(default_factory=lambda: "filterd_producer")
    group_id: str = Field(default_factory=lambda: "filterd_groupid")
    num_messages: int = 50
    timeout: int = 1
    topic: str
    rse_topics: list[str]

    @classmethod
    def load(cls, config_file: str) -> "Config":
        try:
            with open(config_file) as file:
                config_dict = yaml.load(file, Loader=yaml.FullLoader)
            return cls.model_validate(config_dict)
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing {config_file}: {e}") from e
        except Exception as e:
            raise RuntimeError(f"Unexpected error loading {config_file}: {e}") from e
