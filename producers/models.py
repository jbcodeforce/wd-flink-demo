from dataclasses import dataclass, field
from typing import Dict, Optional
from datetime import datetime

@dataclass
class Lead:
    """
    a lead is the primary identity. This class handles the core profile and custom attributes.
    """
    m_id: int
    email: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    lead_score: int = 0
    status: str = "Known"
    # Custom fields like 'Industry' or 'Company' go in attributes
    attributes: Dict[str, any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)

    def get_full_name(self) -> str:
        return f"{self.first_name} {self.last_name}" if self.first_name else self.email



@dataclass
class ActivityRecord:
    """
    The  data feeds are often just a long stream of activities (Email Opens, Web Visits, Form Fills). 
    This class captures the "What" and "When."
    """
    activity_id: int
    lead_id: int
    activity_type: str  # e.g., "Fill Out Form", "Visit Webpage"
    primary_attribute_value: str  # e.g., The name of the form or URL
    timestamp: datetime
    metadata: Dict[str, any] = field(default_factory=dict)

    def is_high_intent(self) -> bool:
        """Logic to flag 'hot' activities for a demo."""
        high_intent_types = ["Fill Out Form", "Click Sales Email"]
        return self.activity_type in high_intent_types


@dataclass
class MarketingProgram:
    """
    everything lives inside a "Program" (e.g., a Webinar, an Ad Campaign, or a Nurture sequence). This class tracks the source of the data.
    """
    program_id: int
    name: str
    channel: str  # e.g., "Webinar", "Social Media", "Email"
    status: str   # e.g., "Active", "Completed"
    workspace: str = "Default"
    
    def calculate_roi(self, revenue: float, cost: float) -> float:
        if cost == 0: return 0.0
        return (revenue - cost) / cost