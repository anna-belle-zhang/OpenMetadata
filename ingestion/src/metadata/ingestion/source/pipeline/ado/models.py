"""Pydantic models for ADO run and approval dump files."""
from datetime import datetime
from enum import Enum
from typing import Literal, Optional

from pydantic import BaseModel, Field


class RunType(str, Enum):
    BUILD = "build"
    DEPLOY = "deploy"


class AdoRun(BaseModel):
    id: int
    name: str
    git_sha: str
    pipeline_type: Literal["image_build", "image_deploy", "infra_deploy"]
    result: Optional[str] = None
    start_time: Optional[datetime] = Field(None, alias="startTime")
    run_type: RunType = Field(alias="type")
    image: Optional[str] = None
    version: Optional[str] = None
    env: Optional[str] = None

    model_config = {"populate_by_name": True}


class AdoApproval(BaseModel):
    run_id: int = Field(alias="runId")
    approver: str
    approved_at: datetime = Field(alias="approvedAt")

    model_config = {"populate_by_name": True}
