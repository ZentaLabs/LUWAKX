import logging
import os

import SimpleITK
import numpy as np
from moosez import moose

logger = logging.getLogger(__name__)


def blur_face(image: SimpleITK.Image, face_mask: SimpleITK.Image, sigma: float = 3.0):
    face_mask_f = SimpleITK.Cast(face_mask, SimpleITK.sitkFloat32)
    image_blurred = SimpleITK.SmoothingRecursiveGaussian(image, sigma)
    image_blurred_face = SimpleITK.Multiply(image_blurred, face_mask_f)

    inverse_mask = SimpleITK.InvertIntensity(face_mask_f, maximum=1.0)
    image_background = SimpleITK.Multiply(image, inverse_mask)

    blended = SimpleITK.Add(image_blurred_face, image_background)

    return SimpleITK.Cast(blended, image.GetPixelID())


def prepare_face_mask(image: SimpleITK.Image | None = None, modality: str | None = None, face_segmentation_path: str | None = None, dilation_margin_mm: float = 15.0) -> SimpleITK.Image:
    if face_segmentation_path and os.path.exists(face_segmentation_path):
        image_face_segmentation = SimpleITK.ReadImage(face_segmentation_path, SimpleITK.sitkUInt8)
    elif image and modality:
        mask, _ = moose(image, f"clin_{modality.lower()}_face")
        if not mask:
            raise RuntimeError(
                f"moose returned no segmentation for model 'clin_{modality.lower()}_face'. "
                "The volume may not contain a face/head region."
            )
        image_face_segmentation = mask[0]
    else:
        raise ValueError("Either the path or image and modality must be provided.")

    # Reject unsupported labels so the caller (pipeline) routes the series to manual review.
    label_stats = SimpleITK.LabelShapeStatisticsImageFilter()
    label_stats.Execute(image_face_segmentation)
    present_labels = set(label_stats.GetLabels())
    unexpected_labels = sorted(present_labels - {1, 2})
    if unexpected_labels:
        raise RuntimeError(
            f"Unexpected mask labels {unexpected_labels}; only label 1 (face) and "
            "label 2 (ears) are supported. Manual review required."
        )

    # Face (label 1) comes from ML and may contain stray components — keep largest only.
    if 1 in present_labels:
        face_label = SimpleITK.Cast(SimpleITK.Equal(image_face_segmentation, 1), SimpleITK.sitkUInt8)
        face_label_largest = keep_largest_component(face_label)
    else:
        face_label_largest = None

    # Ears (label 2) are typically two disjoint blobs — keep all components.
    if 2 in present_labels:
        ears_label = SimpleITK.Cast(SimpleITK.Equal(image_face_segmentation, 2), SimpleITK.sitkUInt8)
    else:
        ears_label = None

    if face_label_largest is not None and ears_label is not None:
        combined_mask = SimpleITK.Or(face_label_largest, ears_label)
    elif face_label_largest is not None:
        combined_mask = face_label_largest
    elif ears_label is not None:
        combined_mask = ears_label
    else:
        # Empty mask raises so the pipeline falls back to copy-without-defacing.
        raise RuntimeError(
            "Face segmentation mask contains no foreground voxels (no label 1 or 2). "
            "The volume may not contain a face/head region."
        )

    combined_mask = SimpleITK.Cast(combined_mask, SimpleITK.sitkUInt8)

    if dilation_margin_mm > 0:
        min_spacing = min(combined_mask.GetSpacing())
        dilation_radius = int(np.ceil(dilation_margin_mm / min_spacing))
        dilate_filter = SimpleITK.BinaryDilateImageFilter()
        dilate_filter.SetKernelRadius(dilation_radius)
        dilate_filter.SetForegroundValue(1)
        combined_mask = dilate_filter.Execute(combined_mask)

    return combined_mask


def pixelate_face(image: SimpleITK.Image, face_mask: SimpleITK.Image, target_block_size_mm: float = 8.5) -> SimpleITK.Image:
    """
    Pixelate the face region of an image using physically-consistent block sizes.

    Args:
        image: The input image to anonymize.
        face_mask: Binary mask indicating the face region.
        target_block_size_mm: Target block size in millimeters for pixelation.
                              Default 8.5mm provides good anonymization across resolutions.

    Returns:
        The image with the face region pixelated.
    """
    spacing = image.GetSpacing()
    downsample_factors = [max(1, int(target_block_size_mm / s)) for s in spacing]

    down_size = [
        max(1, int(sz / f)) for sz, f in zip(image.GetSize(), downsample_factors)
    ]
    down_spacing = tuple(s * f for s, f in zip(spacing, downsample_factors))
    transform = SimpleITK.Transform()
    interpolator = SimpleITK.sitkNearestNeighbor

    image_low_res = SimpleITK.Resample(image, transform=transform, interpolator=interpolator,
                                       size=down_size,
                                       outputSpacing=down_spacing,
                                       outputOrigin=image.GetOrigin(),
                                       outputDirection=image.GetDirection())

    mask_low_res = SimpleITK.Resample(face_mask, transform=transform, interpolator=interpolator,
                                      size=down_size,
                                      outputSpacing=down_spacing,
                                      outputOrigin=face_mask.GetOrigin(),
                                      outputDirection=face_mask.GetDirection())

    image_pixelated_f = SimpleITK.Resample(image_low_res, transform=transform, interpolator=interpolator,
                                           size=image.GetSize(),
                                           outputSpacing=image.GetSpacing(),
                                           outputOrigin=image.GetOrigin(),
                                           outputDirection=image.GetDirection(),
                                           outputPixelType=SimpleITK.sitkFloat32)

    mask_pixelated_f = SimpleITK.Resample(mask_low_res, transform=transform, interpolator=interpolator,
                                          size=image.GetSize(),
                                          outputSpacing=image.GetSpacing(),
                                          outputOrigin=face_mask.GetOrigin(),
                                          outputDirection=face_mask.GetDirection(),
                                          outputPixelType=SimpleITK.sitkFloat32)

    image_f = SimpleITK.Cast(image, SimpleITK.sitkFloat32)
    image_face = SimpleITK.Multiply(image_pixelated_f, mask_pixelated_f)
    image_background = SimpleITK.Multiply(image_f, 1.0 - mask_pixelated_f)
    image_blended = SimpleITK.Add(image_face, image_background)

    return SimpleITK.Cast(image_blended, image.GetPixelID())


def keep_largest_component(label_image: SimpleITK.Image) -> SimpleITK.Image:
    label_image_connected_components = SimpleITK.ConnectedComponent(label_image)
    stats = SimpleITK.LabelShapeStatisticsImageFilter()
    stats.Execute(label_image_connected_components)

    labels = stats.GetLabels()
    if not labels:
        raise RuntimeError(
            "Face segmentation mask contains no foreground voxels. "
            "The volume may not contain a face/head region."
        )
    largest_label = max(labels, key=lambda l: stats.GetPhysicalSize(l))
    largest_component_mask = SimpleITK.Equal(label_image_connected_components, largest_label)
    label_image_largest_component = SimpleITK.Cast(largest_component_mask, label_image.GetPixelID())

    return label_image_largest_component
