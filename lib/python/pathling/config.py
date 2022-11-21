from typing import Optional, Any
from dataclasses import dataclass
from py4j.java_gateway import JavaObject


def config(cls=None, /, *, bean_class):
    def wrap(cls):
        cls.__bean_cls = bean_class
        return dataclass(cls, frozen=True)

    return wrap


class ConfigUtils:

    def __init__(self, jvm):
        self._jvm = jvm

    def to_config_bean(self, config: Any) -> JavaObject:
        assert hasattr(config, "__bean_cls")
        return self._jvm.au.csiro.pathling.library.PytonUtils.toConfigBean(config.__bean_cls,
                                                                           config.__dict__)


@config(bean_class="au.csiro.pathling.config.TerminologyAuthConfiguration")
class AuthConfig:
    token_endpoint: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    scope: Optional[str] = None,
    token_expiry_tolerance: Optional[int] = None
