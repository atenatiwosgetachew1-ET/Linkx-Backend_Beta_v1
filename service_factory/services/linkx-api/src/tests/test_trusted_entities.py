import sys
import unittest
from pathlib import Path


SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class TrustedCatalogTests(unittest.TestCase):
    def test_normalize_trusted_entities_accepts_dynamic_scalar_entries(self):
        from batch_manager.utils.Classified_entities import normalize_trusted_entities

        normalized = normalize_trusted_entities([
            {'ACCOUNTNO': '10002121212012'},
            {'CUSTOM_FIELD': 42, 'FLAG': True},
        ])

        self.assertEqual(normalized[0]['ACCOUNTNO'], '10002121212012')
        self.assertEqual(normalized[1]['CUSTOM_FIELD'], 42)
        self.assertEqual(normalized[1]['FLAG'], True)

    def test_normalize_trusted_entities_rejects_nested_values(self):
        from batch_manager.utils.Classified_entities import TrustedEntitiesValidationError, normalize_trusted_entities

        with self.assertRaises(TrustedEntitiesValidationError):
            normalize_trusted_entities([{'ACCOUNTNO': {'nested': 'nope'}}])


    def test_normalize_risk_entities_accepts_dynamic_scalar_entries(self):
        from batch_manager.utils.Classified_entities import normalize_risk_entities

        normalized = normalize_risk_entities([
            {'ACCOUNTNO': '900001'},
            {'BENACCOUNTNO': '900002', 'FLAG': True},
        ])

        self.assertEqual(normalized[0]['ACCOUNTNO'], '900001')
        self.assertEqual(normalized[1]['BENACCOUNTNO'], '900002')
        self.assertEqual(normalized[1]['FLAG'], True)


if __name__ == '__main__':
    unittest.main()
