class MetrologyError(Exception):
    code = "metrology_error"

    def __init__(self, message, *, field=None, details=None):
        super().__init__(message)
        self.field = field
        self.details = dict(details or {})

    def as_dict(self):
        result = {"code": self.code, "message": str(self)}
        if self.field is not None:
            result["field"] = self.field
        if self.details:
            result["details"] = self.details
        return result


class InputError(MetrologyError):
    code = "invalid_input"


class Conflict(MetrologyError):
    code = "conflict"


class Missing(MetrologyError):
    code = "not_found"


class CalibrationError(MetrologyError):
    code = "calibration_error"


class StateError(MetrologyError):
    code = "invalid_state"


class IntegrityError(MetrologyError):
    code = "integrity_error"
