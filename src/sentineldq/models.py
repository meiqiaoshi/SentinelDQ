from dataclasses import dataclass
from datetime import datetime, timezone
import uuid


def utc_now():
    return datetime.now(timezone.utc)


@dataclass
class Run:
    run_id: str
    started_at: datetime
    finished_at: datetime | None = None
    status: str = "running"

    @classmethod
    def start(cls) -> "Run":
        return cls(
            run_id=str(uuid.uuid4()),
            started_at=utc_now(),
        )

    def finalize(self, status: str = "success"):
        self.status = status
        self.finished_at = utc_now()