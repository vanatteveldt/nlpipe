from typing import Iterable


class Module(object):
    """Abstract base class for NLPipe modules"""
    name = None
    
    def check_status(self):
        """Check the status of this module and return an error if not available (e.g. service or tool not found)"""
        raise NotImplementedError()

    def process(self, text):
        """Process the given text and return the result"""
        raise NotImplementedError()

    def convert(self, id, result, format):
        """Convert the given result to the given format (e.g. 'xml'), if possible or raise an exception if not"""
        raise ValueError("Module {self.name} results cannot be converted to {format}".format(**locals()))

    @classmethod
    def register(cls):
        """Register this module in the nlpipe.module.known_modules"""
        register_module(cls)


class UnknownModuleError(ValueError):
    pass

_known_modules = {}


def register_module(module: Module):
    """Register this module in the nlpipe.module.known_modules"""
    if module.name in _known_modules:
        raise ValueError("Module with name {module.name} already registered: {}"
                         .format(_known_modules[module.name], **locals()))
    _known_modules[module.name] = module


def get_module(name: str) -> Module:
    """Get a module instance corresponding to the given module name"""
    try:
        module_class = _known_modules[name]
    except KeyError:
        raise UnknownModuleError("Unknown module: {name}. Known modules: {}"
                         .format(list(_known_modules.keys()), **locals()))
    return module_class()


def known_modules() -> Iterable[Module]:
    """Get all known modules"""
    # noinspection PyUnresolvedReferences
    import nlpipe.modules  # forces registration
    for name in _known_modules:
        yield get_module(name)
