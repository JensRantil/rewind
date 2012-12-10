# Rewind is an event store server written in Python that talks ZeroMQ.
# Copyright (C) 2012  Jens Rantil
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Configuration file utilities."""
import importlib
import logging


_logger = logging.getLogger(__name__)


class ConfigurationError(Exception):

    """An error thrown when configuration of the application fails."""

    def __init__(self, what):
        self.what = what

    def __str__(self):
        return repr(self.what)


def construct_eventstore(config, args, section=None):
    """Construct the event store to write and write from/to.

    The event store is constructed from an optionally given configuration file
    and/or command line arguments. Command line arguments has higher
    presendence over configuration file attributes.

    If a defined event store is missing, `InMemoryEventStore` will be used and
    a warning will be logged to stderr.

    This function is considered public API since external event stores might
    use it to load other (usually, underlying) event stores.

    Arguments:
    config  -- a configuration dictionary. Derived from
                     `configparser.RawConfigParser`.
    args    -- the arguments given at command line to Rewind.
    section -- the config section to use for event store instantiation.

    returns -- a new event store.

    """
    DEFAULT_SECTION = 'general'
    ESTORE_CLASS_ATTRIBUTE = 'class'

    if config is None:
        _logger.warn("Using InMemoryEventStore. Events are not persisted."
                     " See example config file on how to persist them.")
        # XXX: This is to evade circular dependency between config and
        #      eventstores.
        import rewind.server.eventstores as eventstores
        eventstore = eventstores.InMemoryEventStore()
        return eventstore

    if section is None:
        if DEFAULT_SECTION not in config.sections():
            raise ConfigurationError("Missing default section, `general`.")
        section = config.get(DEFAULT_SECTION, 'storage-backend')

    if section not in config.sections():
        msg = "The section for event store does not exist: {0}"
        raise ConfigurationError(msg.format(section))

    if not config.has_option(section, ESTORE_CLASS_ATTRIBUTE):
        errmsg = 'Configuration option `class` missing for section `{0}`.'
        raise ConfigurationError(errmsg.format(section))

    classpath = config.get(section, ESTORE_CLASS_ATTRIBUTE)
    classpath_pieces = classpath.split('.')
    classname = classpath_pieces[-1]
    modulepath = '.'.join(classpath_pieces[0:-1])

    module = importlib.import_module(modulepath)

    # Instantiating the event store in question using custom arguments
    options = config.options(section)
    if ESTORE_CLASS_ATTRIBUTE in options:
        # Unnecessary argument
        i = options.index(ESTORE_CLASS_ATTRIBUTE)
        del options[i]
    _class = getattr(module, classname)
    customargs = {option: config.get(section, option) for option in options}
    try:
        eventstore = _class.from_config(config, args, **customargs)
    except ConfigurationError as e:
        msg = "Could not instantiate `{0}`: {1}"
        raise ConfigurationError(msg.format(_class, e.what))

    return eventstore


def check_config_options(_class, required_options, optional_options, options):
    """Helper method to check options.

    Arguments:
    _class           -- the original class that takes received the options.
    required_options -- the options that are required. If they are not
                        present, a ConfigurationError is raised. Given as a
                        tuple.
    optional_options -- the options that are optional. Given options that are
                        not present in `optional_options` nor in
                        `required_options` will be logged as unrecognized.
                        Given as a tuple.
    options          -- a dictionary of given options.

    Raises:
    ConfigurationError -- if any required option is missing.

    """
    for opt in required_options:
        if opt not in options:
            msg = "Required option missing: {0}"
            raise ConfigurationError(msg.format(opt))
    for opt in options:
        if opt not in (required_options + optional_options):
            msg = "Unknown config option to `{0}`: {1}"
            _logger.warn(msg.format(_class, opt))
