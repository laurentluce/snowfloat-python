"""Settings tests."""
import unittest

from mock import Mock, patch

import snowfloat.settings

class SettingsTests(unittest.TestCase):
    """Settings tests."""
   
    # pylint: disable=C0103
    def tearDown(self):
        reload(snowfloat.settings)

    @patch('__builtin__.open')
    def test_config_file(self, open_mock):
        """Test with valid config file."""
        mock = Mock()
        mock.readline.side_effect = [
            '[snowfloat]\n',
            'host = test_server:443\n',
            'api_key_id = test_api_key_id\n',
            'api_secret_key = test_api_secret_key\n',
            None]
        open_mock.return_value = mock
        reload(snowfloat.settings)
        self.assertEqual(snowfloat.settings.HOST, 'test_server:443')
        self.assertEqual(snowfloat.settings.API_KEY_ID, 'test_api_key_id')
        self.assertEqual(snowfloat.settings.API_SECRET_KEY,
            'test_api_secret_key')

    @patch('__builtin__.open')
    def test_config_file_missing(self, open_mock):
        """Test with missing config file."""
        open_mock.side_effect = IOError()
        reload(snowfloat.settings)
        self.assertEqual(snowfloat.settings.HOST, 'api.snowfloat.com:443')
        self.assertEqual(snowfloat.settings.API_KEY_ID, '')
        self.assertEqual(snowfloat.settings.API_SECRET_KEY, '')

