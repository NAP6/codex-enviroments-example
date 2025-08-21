from typing import Any, Dict, Optional, List, Tuple
import json
import os
import datetime as _dt
from uuid import uuid4
from pathlib import Path

DEFAULT_ODM_URL: str = "https://odmdds.grouperci.com/DecisionService/rest/Credit_Risk_ES/CO/"
ODM_DATE_FORMAT: str = "%Y-%m-%d"  # salida canónica de fechas para ODM
DEFAULT_MAPPING_PATH: str = str(Path(__file__).with_name("mapping.json"))  # mapping.json junto a esta clase


class odm_controller:
    """
    Controlador que opera nativamente con el esquema de ODM.

    Uso típico:
      ctrl = odm_controller()  # usará mapping.json en la misma carpeta
      ctrl.build_request_from_mapping_file(external_flat=datos)
      print(ctrl.get_request_json())  # JSON listo para enviar
    """

    def __init__(
            self,
            odm_url: str = DEFAULT_ODM_URL,
            headers: Optional[dict] = None,
            timeout: int = 15,
            decision_id: Optional[str] = None,
            mapping_path: Optional[str] = None,  # por defecto: mapping.json junto a esta clase
    ) -> None:
        self.odm_url: str = odm_url
        self.headers: dict = headers or {"Content-Type": "application/json"}
        self.timeout: int = timeout

        self.odm_request: Optional[Dict[str, Any]] = (
            {"__DecisionID__": decision_id, "coRequest": {}} if decision_id else None
        )
        self.odm_response: Optional[Dict[str, Any]] = None

        # Si no se indica, usamos mapping.json en la misma carpeta que este archivo
        self.mapping_path: str = mapping_path or DEFAULT_MAPPING_PATH

    # ============================================================
    # Construir ODMRequest DESDE ARCHIVO DE MAPEO (JSON)
    # Estilo soportado: BY_ODM (claveado por nombres ODM con 'from')
    # ============================================================
    def build_request_from_mapping_file(
            self,
            external_flat: Dict[str, Any],
            mapping_path: Optional[str] = None,
            decision_id: Optional[str] = None,
            validate_required: bool = True,
    ) -> Dict[str, Any]:
        """
        Aplica el mapeo (JSON) a un dict plano del sistema externo y construye el ODMRequest.
        mapping JSON (by_odm):
          {
            "co_fields": { "customerType": {"from": "tipo_cliente", "map_from": {"P":"PARTICULAR"}} },
            "variables": { "double": { "loanAmount": {"from": "monto"} } },
            "constants": { "co_fields": {...}, "variables": { "string": {...} } },
            "required_odm": ["loanAmount", ...]
          }
        """
        if not isinstance(external_flat, dict):
            raise TypeError("external_flat debe ser un dict plano.")
        path = mapping_path or self.mapping_path
        mapping = self._load_mapping_file_json(path)
        odm_req, produced_targets = self._build_by_odm_keys(external_flat, mapping, decision_id)

        if validate_required:
            required_targets = set(mapping.get("required_odm", []) or [])
            missing_required = [t for t in required_targets if t not in produced_targets]
            if missing_required:
                raise ValueError(f"Faltan objetivos ODM requeridos: {missing_required}")

        self.odm_request = odm_req
        return odm_req

    # ============================================================
    # Obtener el request en JSON (string)
    # ============================================================
    def get_request_json(self, indent: int = 2) -> str:
        """Devuelve el último ODMRequest construido en formato JSON (string)."""
        if not self.odm_request:
            raise RuntimeError("Aún no hay un ODMRequest construido.")
        return json.dumps(self.odm_request, ensure_ascii=False, indent=indent)

    # ========================= Internos =========================
    def _load_mapping_file_json(self, path: str) -> Dict[str, Any]:
        if not os.path.exists(path):
            raise FileNotFoundError(f"No existe el archivo de mapeo: {path}")
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _build_by_odm_keys(
            self,
            external_flat: Dict[str, Any],
            mapping: Dict[str, Any],
            decision_id: Optional[str],
    ) -> Tuple[Dict[str, Any], List[str]]:
        """Construye el ODMRequest según mapping 'by_odm'. Devuelve (request, produced_targets)."""
        co: Dict[str, Any] = {}
        produced_targets: List[str] = []

        # Constantes en co_fields
        for k, v in (mapping.get("constants", {}).get("co_fields", {}) or {}).items():
            co[k] = v

        # co_fields (ODM => spec {from,...} o atajo "odm_name": "source_key")
        for odm_name, spec in (mapping.get("co_fields") or {}).items():
            if isinstance(spec, str):
                src_key, cfg = spec, {}
            else:
                src_key = spec.get("from")
                cfg = dict(spec); cfg.pop("from", None)

            if src_key in external_flat:
                val = self._apply_transform(external_flat[src_key], cfg)
                co[odm_name] = val
                produced_targets.append(odm_name)
            else:
                if "default" in cfg:
                    co[odm_name] = cfg["default"]
                    produced_targets.append(odm_name)

        # Inicializa colecciones tipadas
        co["dateVariables"], co["doubleVariables"], co["integerVariables"], co["stringVariables"], co["listOfDoubleVariables"] = [], [], [], [], []

        # Constantes en variables
        for vtype, d in (mapping.get("constants", {}).get("variables", {}) or {}).items():
            for name, value in (d or {}).items():
                self._add_var(co, vtype, name, value)
                produced_targets.append(name)

        # variables (ODM => spec {from,...} o atajo "odm_name": "source_key")
        for vtype, spec_dict in (mapping.get("variables") or {}).items():
            for odm_name, spec in (spec_dict or {}).items():
                if isinstance(spec, str):
                    src_key, cfg = spec, {}
                else:
                    src_key = spec.get("from")
                    cfg = dict(spec); cfg.pop("from", None)

                if src_key in external_flat:
                    val = self._apply_transform(external_flat[src_key], cfg)
                    self._add_var(co, vtype, odm_name, val)
                    produced_targets.append(odm_name)
                else:
                    if "default" in cfg:
                        self._add_var(co, vtype, odm_name, cfg["default"])
                        produced_targets.append(odm_name)

        # Limpia colecciones vacías
        for k in ("dateVariables", "doubleVariables", "integerVariables", "stringVariables", "listOfDoubleVariables"):
            if not co[k]:
                co.pop(k)

        # DecisionID
        dec_id = decision_id or (self.odm_request or {}).get("__DecisionID__") or f"Decision_{uuid4().hex[:12]}"
        odm_req = {"__DecisionID__": dec_id, "coRequest": co}
        return odm_req, produced_targets

    # ===== Helpers de transformaciones =====
    def _apply_transform(self, value: Any, cfg: Dict[str, Any]) -> Any:
        """
        Transformaciones soportadas (opcionales en mapping):
          - default: valor por defecto si el input viene vacío
          - map_from: dict de sustitución exacta (origen -> ODM) (alias 'map')
          - split_csv: "1, 2;3" -> ["1","2","3"] (se castea después si es list_double)
          - date_in: formato de entrada (salida siempre ODM_DATE_FORMAT)
          - strip | upper | lower: sobre strings
          - bool_map: {"S": true, "N": false}
          - scale: multiplica (para números)
        """
        if (value is None or (isinstance(value, str) and value.strip() == "")) and "default" in cfg:
            value = cfg["default"]

        # Mapeo exacto de valores de entrada
        map_from = cfg.get("map_from") or cfg.get("map")
        if isinstance(map_from, dict):
            value = map_from.get(value, value)

        # CSV -> lista
        if cfg.get("split_csv") and isinstance(value, str):
            tokens = [t.strip() for t in value.replace(";", ",").split(",") if t.strip()]
            value = tokens

        # Fechas: se especifica 'date_in'; salida fija ODM_DATE_FORMAT
        if "date_in" in cfg or cfg.get("as_date"):
            value = self._normalize_date(value, cfg.get("date_in"))

        # Strings
        if isinstance(value, str):
            if cfg.get("strip"): value = value.strip()
            if cfg.get("upper"): value = value.upper()
            if cfg.get("lower"): value = value.lower()

        # Booleano mapeado
        if isinstance(cfg.get("bool_map"), dict):
            value = cfg["bool_map"].get(value, bool(value))

        # Escala numérica
        if "scale" in cfg and value is not None:
            try:
                value = float(value) * float(cfg["scale"])
            except Exception:
                pass

        return value

    def _add_var(self, co: Dict[str, Any], vtype: str, name: str, value: Any) -> None:
        if value is None:
            return
        if vtype == "date":
            (co.setdefault("dateVariables", [])).append({"name": name, "value": str(value)})
        elif vtype == "double":
            (co.setdefault("doubleVariables", [])).append({"name": name, "value": float(value)})
        elif vtype == "integer":
            (co.setdefault("integerVariables", [])).append({"name": name, "value": int(value)})
        elif vtype == "string":
            (co.setdefault("stringVariables", [])).append({"name": name, "value": str(value)})
        elif vtype == "list_double":
            if isinstance(value, list):
                vals = [float(x) for x in value]
            else:
                vals = [float(x) for x in (value or [])]
            (co.setdefault("listOfDoubleVariables", [])).append({"name": name, "value": vals})
        else:
            raise ValueError(f"Tipo de variable no soportado: {vtype}")

    def _normalize_date(self, v: Any, fmt_in: Optional[str]) -> Optional[str]:
        if v is None or v == "":
            return None
        if isinstance(v, _dt.date):
            if isinstance(v, _dt.datetime):
                v = v.date()
            return v.strftime(ODM_DATE_FORMAT)
        if isinstance(v, str):
            s = v.strip()
            if fmt_in:
                return _dt.datetime.strptime(s, fmt_in).date().strftime(ODM_DATE_FORMAT)
            return _dt.date.fromisoformat(s[:10]).strftime(ODM_DATE_FORMAT)
        raise ValueError(f"Fecha inválida ({type(v)})")





if __name__ == "__main__":
    # Diccionario de ejemplo con los nombres de variables definidos en mapping.json
    external_data = {
        "tipo_activo": "CAR",
        "tipo_cliente": "P",
        "estado_civil": "SOLTERO",
        "tipo_pago": "CONTADO",
        "tipo_producto": "AUTO",
        "pyme_score": 700,
        "rci_tipo_cliente": "E",
        "risk_score": 500,
        "rol": "TITULAR",
        "tipo_vehiculo": "SUV",
        "tipo_trabajo": "ASALARIADO",
        "fecha_decision": "15/08/2023",

        "fecha_solicitud": "01/08/2023",
        "fecha_matriculacion": "01/01/2020",
        "fecha_ultimo_impago": "10/05/2021",
        "fecha_ultima_recuperacion": "20/06/2021",
        "fecha_nacimiento": "05/10/1990",

        "saldo_pendiente": 20000.0,
        "monto_solicitado": 30000.0,
        "valor_eurotax": 15000.0,
        "ingreso_anual_neto": 40000.0,
        "entrada": 5000.0,
        "ingreso_mensual_neto": 3500.0,
        "cuota_mensual": 600.0,
        "ratio_44": 0.25,

        "plazo_meses": 36,
        "antiguedad_laboral_anios": 5,
        "total_incidentes": 0,
        "litigios_impagos_actuales": 0,
        "numero_clientes": 1,

        "cp_tienda": "28080",
        "cp": "28010",
        "codigo_pais": "es",

        "plan_amortizacion": "1.0,2.56,0.0,4.3"
    }

    # Crear instancia del controlador
    ctrl = odm_controller()

    # Construir el request desde el archivo mapping.json
    ctrl.build_request_from_mapping_file(external_flat=external_data)

    # Imprimir el JSON resultante
    print(ctrl.get_request_json(indent=2))
