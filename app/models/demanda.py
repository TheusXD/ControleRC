# Pydantic models
from datetime import datetime
from typing import Optional, Literal, Any
from pydantic import BaseModel, Field, constr

class Demanda(BaseModel):
    id: Optional[str] = None  # Firestore document ID
    solicitante_demanda: str
    descricao_necessidade: constr(min_length=5, max_length=500)
    categoria: constr(max_length=50)
    anexo_path: Optional[str] = None
    status_demanda: Literal["Aberta", "Em Atendimento", "Fechada"] = "Aberta"
    created_at: datetime = Field(default_factory=datetime.now)

    class Config:
        arbitrary_types_allowed = True