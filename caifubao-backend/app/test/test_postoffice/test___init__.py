import unittest
from unittest import TestCase
from app.lib.postoffice import PostOffice


class TestPostOfficeExtractMimeInfo(TestCase):
    def test_valid_text_file(self):
        self.assertEqual(PostOffice.extract_mime_info("file1.txt"), ("file1.txt", "txt", "text"))

    def test_valid_image_file(self):
        self.assertEqual(PostOffice.extract_mime_info("image.jpg"), ("image.jpg", "jpg", "image"))

    def test_invalid_extension(self):
        self.assertIsNone(PostOffice.extract_mime_info("file.docx"))

    def test_empty_file_name(self):
        self.assertIsNone(PostOffice.extract_mime_info(""))

    def test_multiple_dots_in_file_name(self):
        self.assertEqual(PostOffice.extract_mime_info("file.name.txt"), ("file.name.txt", "txt", "text"))

    def test_uppercase_extension(self):
        self.assertEqual(PostOffice.extract_mime_info("FILE.JPG"), ("FILE.JPG", "jpg", "image"))

    def test_no_extension(self):
        self.assertIsNone(PostOffice.extract_mime_info("file"))

    def test_file_name_with_spaces(self):
        self.assertEqual(PostOffice.extract_mime_info("my file.txt"), ("my file.txt", "txt", "text"))

    def test_file_name_with_special_characters(self):
        self.assertEqual(PostOffice.extract_mime_info("file!.jpg"), ("file!.jpg", "jpg", "image"))

    def test_file_name_with_leading_trailing_spaces(self):
        self.assertEqual(PostOffice.extract_mime_info("  file.txt  "), ("file.txt", "txt", "text"))


if __name__ == "__main__":
    unittest.main()
