import SimpleITK
import image_anonymization
import argparse
import os


def get_input_name(path: str) -> str:
    base = os.path.basename(path.rstrip(os.sep))
    if base.endswith(".nii.gz"):
        return base[:-7]
    if base.endswith(".nii"):
        return base[:-4]
    return base


def read_dicom_directory(dicom_dir: str) -> tuple[SimpleITK.Image, str | None]:
    if not os.path.isdir(dicom_dir):
        raise ValueError(f"Not a directory: {dicom_dir}")

    reader = SimpleITK.ImageSeriesReader()
    series_ids = reader.GetGDCMSeriesIDs(dicom_dir)

    if not series_ids:
        raise ValueError(f"No DICOM series found in: {dicom_dir}")

    dicom_filenames = reader.GetGDCMSeriesFileNames(dicom_dir, series_ids[0])

    reader.SetFileNames(dicom_filenames)
    image = reader.Execute()
    modality = image.GetMetaData("0008|0060") if image.HasMetaDataKey("0008|0060") else None

    return image, modality


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Deface NIfTI or DICOM images.")

    parser.add_argument("-i", "--input", required=True,
                        type=str,
                        help="Path to input (NIfTI file or DICOM directory).")

    parser.add_argument("-o", "--output",
                        type=str,
                        help="Path to output directory.")

    parser.add_argument("-fm", "--face_mask_path",
                        type=str,
                        help="Path to the existing face mask you want to use.")

    parser.add_argument("-m", "--modality",
                        type=str,
                        help="The modality of the image. Only required for NIfTI images.")

    parser.add_argument("-sfm", "--save_face_mask",
                        action="store_true",
                        help="Specify if you want to save the face mask. It will be saved to the output directory.")

    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()

    input_path = args.input
    input_name = get_input_name(input_path)
    if not os.path.exists(input_path):
        parser.error(f"Input path does not exist: {input_path}")
    print(f"Input\n Path: {input_path}\n Name: {input_name}")

    output_path = args.output
    if not output_path:
        output_path = os.path.dirname(input_path)
    os.makedirs(output_path, exist_ok=True)
    print(f"Output\n Path: {output_path}")

    modality = args.modality
    if os.path.isfile(input_path):
        if (input_path.endswith(".nii") or input_path.endswith(".nii.gz")) and modality:
            image = SimpleITK.ReadImage(input_path)
        else:
            parser.error(f"File must be a NIfTI (.nii or .nii.gz) and modality must be provided.\n Got: {input_path} and modality {modality}")

    elif os.path.isdir(input_path):
        image, modality = read_dicom_directory(input_path)

    else:
        parser.error(f"Invalid input path: {input_path}")

    face_mask_path = args.face_mask_path
    if face_mask_path:
        print(f"Getting face mask from: {face_mask_path}")
    image_face_segmentation = image_anonymization.prepare_face_mask(image, modality, face_mask_path)

    save_face_mask = args.save_face_mask
    if save_face_mask and not face_mask_path:
        face_mask_out_path = os.path.join(output_path, f"{input_name}_face_mask.nii.gz")
        print(f"Saving face mask to: {face_mask_out_path}")
        SimpleITK.WriteImage(image_face_segmentation, face_mask_out_path)

    image_defaced = image_anonymization.pixelate_face(image, image_face_segmentation)
    image_defaced_path = os.path.join(output_path, f"{input_name}_defaced.nii.gz")
    print(f"Saving defaced image to: {image_defaced_path}")
    SimpleITK.WriteImage(image_defaced, image_defaced_path)


if __name__ == "__main__":
    main()
