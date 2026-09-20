from image_stages import Grayscale, GaussianBlur, Sharpen
from ppm_image import PPMImage
from std.testing import assert_equal


def main() raises:
    # Include tiny images, SIMD batches, and a scalar tail.
    for width in [1, 2, 19]:
        var original = PPMImage(width, 5, UInt8(73))
        var copied = original.copy()
        copied.set_pixel(0, 0, 0, 0, 0)
        assert_equal(original.get_r(0, 0), UInt8(73))
        assert_equal(copied.get_g(0, 0), UInt8(0))
        assert_equal(copied.get_b(0, 0), UInt8(0))

        var gray = Grayscale()
        var blur = GaussianBlur()
        var sharpen = Sharpen()
        var gray_result = gray.compute(original^)
        assert_equal(gray_result.value().checksum(), UInt64(width * 5 * 3 * 73))
        var blur_result = blur.compute(gray_result.take())
        assert_equal(blur_result.value().checksum(), UInt64(width * 5 * 3 * 73))
        var sharp_result = sharpen.compute(blur_result.take())
        var result = sharp_result.take()
        assert_equal(result.checksum(), UInt64(width * 5 * 3 * 73))
        for y in range(5):
            for x in range(width):
                assert_equal(result.get_r(x, y), UInt8(73))
                assert_equal(result.get_g(x, y), UInt8(73))
                assert_equal(result.get_b(x, y), UInt8(73))
    print("PASS: image copy/move and grayscale/blur/sharpen")
