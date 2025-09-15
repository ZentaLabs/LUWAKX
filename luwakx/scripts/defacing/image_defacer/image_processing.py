import SimpleITK


def image_geometries_identical(reference_image: SimpleITK.Image, image: SimpleITK.Image) -> bool:
    reference_geometry = (reference_image.GetSize(), reference_image.GetSpacing(), reference_image.GetOrigin(),
                          reference_image.GetDirection())
    image_geometry = (image.GetSize(), image.GetSpacing(), image.GetOrigin(), image.GetDirection())
    return reference_geometry == image_geometry


def reslice_identity(reference_image: SimpleITK.Image, moving_image: SimpleITK.Image,
                     output_image_path: str | None = None, is_label_image: bool = False) -> SimpleITK.Image:
    if image_geometries_identical(reference_image, moving_image):
        return moving_image

    resampler = SimpleITK.ResampleImageFilter()
    resampler.SetReferenceImage(reference_image)

    if is_label_image:
        resampler.SetInterpolator(SimpleITK.sitkNearestNeighbor)
        output_pixel_type = SimpleITK.sitkUInt8
    else:
        resampler.SetInterpolator(SimpleITK.sitkLinear)
        output_pixel_type = moving_image.GetPixelID()

    resampled_image = resampler.Execute(moving_image)
    resampled_image = SimpleITK.Cast(resampled_image, output_pixel_type)

    if output_image_path is not None:
        SimpleITK.WriteImage(resampled_image, output_image_path)

    return resampled_image
