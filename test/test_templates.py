import json
import unittest

import jinja2

test_status = '''
{
    "CE04OSPS-SF01B-2A-CTDPFA107": [
      {
        "arrow": "&nearr;",
        "elapsed": 2.64296,
        "expected_rate": 1.0,
        "fail_interval": 600,
        "five_minute_rate": 2.8001113666515214,
        "id": 2514,
        "last_status": "OPERATIONAL",
        "method": "streamed",
        "new_status": "DEGRADED",
        "one_day_rate": 3.975965778173281,
        "stream": "ctdpf_sbe43_sample",
        "warn_interval": 120,
        "color": "orange"
      }
    ],
    "CE04OSPS-SF01B-3A-FLORTD104": [
      {
        "arrow": "&nearr;",
        "elapsed": 4.83731,
        "expected_rate": 0.8,
        "fail_interval": 600,
        "five_minute_rate": 0.6222469703670047,
        "id": 1070,
        "last_status": "DEGRADED",
        "method": "streamed",
        "new_status": "FAILED",
        "one_day_rate": 0.8289498062248218,
        "stream": "flort_d_data_record",
        "warn_interval": 120,
        "color": "red"
      }
    ],
    "CE04OSPS-SF01B-4B-VELPTD106": [
      {
        "arrow": "&nearr;",
        "elapsed": 3.596959,
        "expected_rate": 3.2,
        "fail_interval": 600,
        "five_minute_rate": 2.7862219253486864,
        "id": 1369,
        "last_status": "DEGRADED",
        "method": "streamed",
        "new_status": "OPERATIONAL",
        "one_day_rate": 3.975977344215421,
        "stream": "velpt_velocity_data",
        "warn_interval": 120,
        "color": "black"
      }
    ],
    "RS01SLBS-LJ01A-05-HPIESA101": [
      {
        "arrow": "&nearr;",
        "elapsed": 87.458971,
        "expected_rate": 0.796,
        "fail_interval": 600,
        "five_minute_rate": 0.8500338077334976,
        "id": 1766,
        "last_status": "DEGRADED",
        "method": "streamed",
        "new_status": "OPERATIONAL",
        "one_day_rate": 0.790053206507585,
        "stream": "horizontal_electric_field",
        "warn_interval": 120,
        "color": "black"
      },
      {
        "arrow": "&nearr;",
        "elapsed": 79.486324,
        "expected_rate": 0.0167,
        "fail_interval": 5999,
        "five_minute_rate": 0.016667329563401913,
        "id": 1767,
        "last_status": "DEGRADED",
        "method": "streamed",
        "new_status": "OPERATIONAL",
        "one_day_rate": 0.016655100681778452,
        "stream": "hpies_data_header",
        "warn_interval": 1200,
        "color": "black"
      },
      {
        "arrow": "&nearr;",
        "elapsed": 78.899055,
        "expected_rate": 0.187,
        "fail_interval": 600,
        "five_minute_rate": 0.18334062519742106,
        "id": 1769,
        "last_status": "DEGRADED",
        "method": "streamed",
        "new_status": "OPERATIONAL",
        "one_day_rate": 0.18320610749956298,
        "stream": "motor_current",
        "warn_interval": 120,
        "color": "black"
      }
    ]
  }
'''

test_status = json.loads(test_status)


class TemplateTest(unittest.TestCase):
    def setUp(self):
        loader = jinja2.PackageLoader('ooi_status', 'templates')
        self.env = jinja2.Environment(loader=loader, trim_blocks=True)

    def test_plaintext_template(self):
        template = self.env.get_template('plaintext_status.jinja')
        print template.render(status_dict=test_status, base_url='http://localhost')

    def test_html_template(self):
        template = self.env.get_template('html_status.jinja')
        text = template.render(status_dict=test_status, base_url='http://localhost')
        open('test.html', 'w').write(text)