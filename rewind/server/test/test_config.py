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

"""Test initialization from configuration."""

try:
    # Python < 3
    import ConfigParser as configparser
except ImportError:
    # Python >= 3
    import configparser
import unittest

import rewind.server.eventstores as eventstores
import rewind.server.config as rconfig


class TestInitialization(unittest.TestCase):

    """Test `rconfig.construct_eventstore(...)` behaviour."""

    def testInMemoryFallback(self):
        """Test `construct_eventstore(...)` defaults to in-memory estore."""
        estore = rconfig.construct_eventstore(None, [])
        self.assertIsInstance(estore, eventstores.InMemoryEventStore)

    def testStringRepresentationOfConfigurationError(self):
        """Test ConfigurationError.__str__ behaviour.

        It's not crucial behaviour, but always worth the coverage.

        """
        err = rconfig.ConfigurationError("Bad string")
        self.assertEquals(str(err), "'Bad string'")

    def testMissingDefaultSection(self):
        """Test `construct_eventstore(...)` bails on no default section."""
        config = configparser.ConfigParser()
        self.assertRaises(rconfig.ConfigurationError,
                          rconfig.construct_eventstore,
                          config, [])

    def testMissingConfigEventStoreSection(self):
        """Test `construct_eventstore(...)` bails on missing class section."""
        config = configparser.ConfigParser()
        config.add_section("general")
        config.set("general", "storage-backend", "nonexistsection")
        self.assertRaises(rconfig.ConfigurationError,
                          rconfig.construct_eventstore,
                          config, [])

    def testMissingArgumentEventStoreSection(self):
        """Test `construct_eventstore(...)` bails on missing arg section."""
        config = configparser.ConfigParser()
        self.assertRaises(rconfig.ConfigurationError,
                          rconfig.construct_eventstore,
                          config, [], "nonexistsection")

    def testMissingEventStoreClass(self):
        """Test `construct_eventstore(...)` bails on missing class path."""
        config = configparser.ConfigParser()
        config.add_section("general")
        config.set("general", "storage-backend", "estoresection")
        config.add_section("estoresection")
        self.assertRaises(rconfig.ConfigurationError,
                          rconfig.construct_eventstore,
                          config, [])

    def testCreatingInMemoryStoreUsingConfig(self):
        """Full test of `construct_eventstore(...)`."""
        config = configparser.ConfigParser()
        config.add_section("general")
        config.set("general", "storage-backend", "estoresection")
        config.add_section("estoresection")

        config.set("estoresection", "class",
                   "rewind.server.eventstores.InMemoryEventStore")

        estore = rconfig.construct_eventstore(config, [])

        self.assertIsInstance(estore, eventstores.InMemoryEventStore)

    def testCreatingInMemoryStoreUsingConfigWithGivenSection(self):
        """Test full test of `construct_eventstore(...)` given a section."""
        config = configparser.ConfigParser()
        config.add_section("estoresection")
        # The 'general' section need not to be defined here since a section is
        # given to `construct_eventstore(...)` below.

        config.set("estoresection", "class",
                   "rewind.server.eventstores.InMemoryEventStore")

        estore = rconfig.construct_eventstore(config, [], "estoresection")

        self.assertIsInstance(estore, eventstores.InMemoryEventStore)
