import unittest

from generator.generate_data import SAMPLE_LINES


class GenerateDataTest(unittest.TestCase):
    def test_uses_assignment_sample_lines(self) -> None:
        self.assertEqual(len(SAMPLE_LINES), 8)
        self.assertEqual(
            SAMPLE_LINES[0],
            "748c35aa-854c-4efa-ab52-f90e09f6d8eb|20001 00001|23.7367148|40.3515728|1718175430523",
        )
        self.assertEqual(
            SAMPLE_LINES[-1],
            "dd48baff-c9fb-40d0-8e5a-2b5f06b88f32|60001_00001|6.24584545|43.0772907|1718175437593",
        )


if __name__ == "__main__":
    unittest.main()
