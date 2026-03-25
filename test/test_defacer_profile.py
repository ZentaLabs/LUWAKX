import zipfile
import numpy as np
import vedo
import SimpleITK as sitk
import unittest
import os
import shutil
import pydicom
import tempfile
import json
import sys
import warnings

# Suppress deprecation warnings from batchgenerators/scipy
warnings.filterwarnings("ignore", category=DeprecationWarning, module="batchgenerators")

from luwakx.anonymize import LuwakAnonymizer
from luwakx.deface_service import DefaceService
from luwakx.dicom_series import DicomSeries
from luwakx.dicom_file import DicomFile
from luwakx.luwak_logger import get_logger, setup_logger
from luwakx.utils import has_gpu, download_github_asset_by_tag

class TestDefacerProfile(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a temporary output directory
        cls.test_output_dir = "test_output"

        # Path to the decompressed test data directory
        cls.test_data_dir = "test_data_defacer"
        cls.test_volume_dir = os.path.join(cls.test_data_dir, "test_volume")

        # Create required directories
        os.makedirs(cls.test_volume_dir, exist_ok=True)
        token = os.environ.get("TEST_DATA_TOKEN")
        # Download and extract CT_Vol_002_STD_dcm.zip into test_data_defacer/test_volume
        dcm_zip_path = os.path.join(cls.test_data_dir, "CT_Vol_002_STD_dcm.zip")
        if not os.path.exists(os.path.join(cls.test_volume_dir, "CT_Vol_002_STD_dcm")):
            download_github_asset_by_tag(
                "ZentaLabs", "luwak", "testing-data", "CT_Vol_002_STD_dcm.zip", dcm_zip_path, token
            )
            target_dir = os.path.join(cls.test_volume_dir, "CT_Vol_002_STD_dcm")
            os.makedirs(target_dir, exist_ok=True)
            with zipfile.ZipFile(dcm_zip_path, "r") as zip_ref:
                zip_ref.extractall(target_dir)
            os.remove(dcm_zip_path)

        # Download CT_Vol_002_STD_face_mask.nrrd into test_data_defacer/
        nii_path = os.path.join(cls.test_data_dir, "CT_Vol_002_STD_face_mask.nrrd")
        if not os.path.exists(nii_path):
            download_github_asset_by_tag(
                "ZentaLabs", "luwak", "testing-data", "CT_Vol_002_STD_face_mask.nrrd", nii_path, token
            )

    @classmethod
    def tearDownClass(cls):
        # Clean up output directory after all tests
        if os.path.exists(cls.test_output_dir):
            shutil.rmtree(cls.test_output_dir)

    @classmethod
    def create_test_config(cls, input_folder, output_folder, useExistingMaskDefacer=[]):
        # Helper to create a config file for defacer
        if not os.path.isabs(input_folder):
            input_folder = os.path.abspath(input_folder)
        if not os.path.isabs(output_folder):
            output_folder = os.path.abspath(output_folder)
        output_private_mapping_folder = os.path.join(output_folder, "private")

        config = {
            "inputFolder": input_folder,
            "outputDeidentifiedFolder": output_folder,
            "outputPrivateMappingFolder": output_private_mapping_folder,
            "recipesFolder": os.path.join(output_folder, "recipes"),
            "recipes":  ["clean_recognizable_visual_features"],
            "testOptions": {"useExistingMaskDefacer": useExistingMaskDefacer}
        }
        config_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(config, config_file, indent=2)
        config_file.close()
        return config_file.name

    def setUp(self):
        # Ensure output directory is clean before each test
        if os.path.exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)
        os.makedirs(self.test_output_dir, exist_ok=True)

        # Initialize logger
        log_file_path = os.path.join(self.test_output_dir, 'luwak_test.log')
        setup_logger(log_level='INFO', log_file=log_file_path, console_output=False)
        self.logger = get_logger('test_defacer_profile')

        print("\n######################START TEST######################")

    def test_defacer_service_makes_defacing(self):    
        # Test the defacer service directly without full anonymization
        print("Testing defacer service directly...")
        # Simple GPU check
        HAS_GPU = has_gpu()

        if not HAS_GPU:
            useExistingMaskDefacer = os.path.abspath(os.path.join(self.test_data_dir, "CT_Vol_002_STD_face_mask.nrrd"))
            config_path = self.create_test_config(self.test_volume_dir, self.test_output_dir, [useExistingMaskDefacer])
        else:
            config_path = self.create_test_config(self.test_volume_dir, self.test_output_dir)
        
        # Load config and setup
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Create DefaceService
        deface_service = DefaceService(config, self.logger)
        
        # Get DICOM files (assuming they're all one series)
        from deid.dicom import get_files
        dicom_files = list(get_files(self.test_volume_dir))
        
        # Read first file to get series info
        first_ds = pydicom.dcmread(dicom_files[0], stop_before_pixels=True)
        series_uid = first_ds.SeriesInstanceUID
        modality = getattr(first_ds, 'Modality', None)
        patient_id = getattr(first_ds, 'PatientID', 'TEST_PATIENT')
        patient_name = getattr(first_ds, 'PatientName', '')
        patient_birthdate = getattr(first_ds, 'PatientBirthDate', '')
        study_uid = getattr(first_ds, 'StudyInstanceUID', 'TEST_STUDY')
        
        # Create DicomSeries object with new constructor
        series = DicomSeries(
            original_patient_id=patient_id,
            original_patient_name=str(patient_name),
            original_patient_birthdate=str(patient_birthdate),
            original_study_uid=study_uid,
            original_series_uid=series_uid
        )
        series.modality = modality
        
        # Set up separate directories for each stage (like ProcessingPipeline does)
        organized_temp_dir = os.path.join(self.test_output_dir, "organized")
        defaced_temp_dir = os.path.join(self.test_output_dir, "defaced")
        
        series.organized_base_path = organized_temp_dir
        series.defaced_base_path = defaced_temp_dir
        series.output_base_path = self.test_output_dir
        
        # Add DicomFile objects to series
        for filepath in dicom_files:
            dicom_file = DicomFile(filepath, series_uid)
            # Set organized path (simulate organization stage)
            organized_path = os.path.join(organized_temp_dir, series_uid, os.path.basename(filepath))
            os.makedirs(os.path.dirname(organized_path), exist_ok=True)
            shutil.copy2(filepath, organized_path)
            dicom_file.set_organized_path(organized_path)
            series.add_file(dicom_file)
        
        # Run defacing
        result = deface_service.process_series(series)
        processed_files = result.get('defaced_dicom_files', [])
                
        self.assertTrue(processed_files, "No files processed by defacer!")
        
        # Check what NRRD paths were returned
        nrrd_image = result.get('nrrd_image_path')
        nrrd_defaced = result.get('nrrd_defaced_path')

        # --- Non-face pixel integrity check ---
        # Read original DICOM series (files are in organized_temp_dir/series_uid/)
        original_dicom_dir = os.path.join(organized_temp_dir, series_uid)
        reader = sitk.ImageSeriesReader()
        original_series_files = reader.GetGDCMSeriesFileNames(original_dicom_dir)
        reader.SetFileNames(original_series_files)
        original_vol = reader.Execute()
        original_arr = sitk.GetArrayFromImage(original_vol)

        # Read defaced DICOM series
        reader = sitk.ImageSeriesReader()
        defaced_series_files = reader.GetGDCMSeriesFileNames(defaced_temp_dir)
        reader.SetFileNames(defaced_series_files)
        defaced_arr = sitk.GetArrayFromImage(reader.Execute())

        # Load face mask 
        face_mask_path = os.path.abspath(os.path.join(self.test_data_dir, "CT_Vol_002_STD_face_mask.nrrd"))
        face_mask_img = sitk.ReadImage(face_mask_path)
        # Dilate the mask to account for pixelation block boundary effects.
        # Blocks at the mask border will always affect pixels up to block_size voxels
        # beyond the mask edge. Compute the dilation radius from the physical block
        # size and the volume's minimum voxel spacing.
        block_size_mm = config.get('physicalFacePixelationSizeMm', 8.5)
        min_spacing = min(original_vol.GetSpacing())
        dilation_radius = int(np.ceil(block_size_mm / min_spacing))
        self.logger.info(f"Face mask dilation radius: {dilation_radius} voxels (block={block_size_mm}mm, spacing={min_spacing:.2f}mm)")
        dilate_filter = sitk.BinaryDilateImageFilter()
        dilate_filter.SetKernelRadius(dilation_radius)
        dilate_filter.SetForegroundValue(1)
        face_mask_dilated_vol = dilate_filter.Execute(sitk.Cast(face_mask_img, sitk.sitkUInt8))
        face_mask_dilated = sitk.GetArrayFromImage(face_mask_dilated_vol).astype(bool)

        # Check shapes
        self.assertEqual(original_arr.shape, defaced_arr.shape, "Original and defaced volumes have different shapes!")
        self.assertEqual(original_arr.shape, face_mask_dilated.shape, "Face mask shape does not match volume shape!")

        # Compare only non-face voxels (outside dilated mask): allow atol=1e-4
        nonface_voxels = ~face_mask_dilated
        unchanged = np.isclose(original_arr[nonface_voxels], defaced_arr[nonface_voxels], atol=1e-4)
        n_changed = int(np.sum(~unchanged))
        self.assertTrue(
            np.all(unchanged),
            f"Non-face pixels were modified by defacing! ({n_changed} voxels differ outside dilated face mask)"
        )
        # Verify NRRD files exist
        if nrrd_image and nrrd_defaced:
            self.assertTrue(os.path.exists(nrrd_image), f"NRRD image not found at {nrrd_image}")
            self.assertTrue(os.path.exists(nrrd_defaced), f"NRRD defaced not found at {nrrd_defaced}")
            if not HAS_GPU:
                return

            # Plot image.nrrd and image_defaced.nrrd side by side
            image = sitk.ReadImage(nrrd_image)
            defaced = sitk.ReadImage(nrrd_defaced)
            array = sitk.GetArrayFromImage(image)  # shape: [slices, height, width]
            defaced_array = sitk.GetArrayFromImage(defaced)  # shape: [slices, height, width]
            # Restrict x-axis (width) to half:end
            x_half = array.shape[2] // 2
            array_cropped = array[:x_half, :, :]
            defaced_array_cropped = defaced_array[:x_half, :, :]
            # Normalize data to [0, 1] for better contrast
            array_norm = (array_cropped - array_cropped.min()) / (array_cropped.max() - array_cropped.min())
            defaced_array_norm = (defaced_array_cropped - defaced_array_cropped.min()) / (defaced_array_cropped.max() - defaced_array_cropped.min())
            spacing = image.GetSpacing()  # (x, y, z) or (width, height, slice)
            # vedo expects spacing in (z, y, x) order for numpy arrays
            spacing = spacing[::-1]
            threshold = 0.42
            vol1 = vedo.Volume(array_norm, spacing=spacing)
            vol2 = vedo.Volume(defaced_array_norm, spacing=spacing)
            # Create isosurfaces for both volumes
            mesh1 = vol1.isosurface(threshold)
            mesh2 = vol2.isosurface(threshold)

            # Position meshes side by side
            # Center of the mesh
            center = mesh1.center_of_mass()
            mesh1.pos(-center[0], -center[1], -center[2])
            center2 = mesh2.center_of_mass()
            bounds1 = mesh1.bounds()
            offset = bounds1[1] - bounds1[0] + 10  # Use mesh1's width for spacing
            mesh2.pos(-center2[0] + offset, -center2[1], -center2[2])
            mesh1.pos(-center[0]+offset, -center[1], -center[2])
            plt = vedo.Plotter(N=2, axes=1, size=(1200,600))
            mesh1.rotate(angle=90, axis=[1, 0, 0])
            mesh1.rotate(angle=180, axis=[0, 0, 1])
            mesh2.rotate(angle=90, axis=[1, 0, 0])
            mesh2.rotate(angle=180, axis=[0, 0, 1])
            plt.add(vedo.Text2D("Original", pos="top-left", c="white", bg="black"), at=0)
            plt.add(vedo.Text2D("Defaced", pos="top-left", c="white", bg="black"), at=1)

            # Camera position: move along negative y, looking back at the center
            cam_pos = [center[0], center[1], center[2]+1000]  
            plt.camera.SetPosition(cam_pos)
            plt.camera.SetFocalPoint(center)
            plt.camera.SetViewUp([1, 0, 0])
            plt.show(mesh1, at=0)
            plt.show(mesh2, at=1, interactive=True)
            plt.close()
        else:
            self.fail("No NRRD files returned by defacer")

    def test_luwak_defacer_recipe(self):
        # Test the full luwak anonymization with defacer recipe
        print("Testing luwak full anonymization with defacer recipe...")
        # Simple GPU check
        HAS_GPU = has_gpu()

        if not HAS_GPU:
            useExistingMaskDefacer = os.path.abspath(os.path.join(self.test_data_dir, "CT_Vol_002_STD_face_mask.nrrd"))
            config_path = self.create_test_config(self.test_volume_dir, self.test_output_dir, [useExistingMaskDefacer])
        else:
            config_path = self.create_test_config(self.test_volume_dir, self.test_output_dir)

        anonymizer = LuwakAnonymizer(config_path)
        coordinator = anonymizer.anonymize()
        # Find all output DICOM files
        output_files = []
        for root, dirs, files in os.walk(self.test_output_dir):
            for name in files:
                if name.endswith('.dcm'):
                    output_files.append(os.path.join(root, name))
        self.assertTrue(output_files, "No output DICOM files found after luwak anonymization!")
        for fpath in output_files:
            ds = pydicom.dcmread(fpath, stop_before_pixels=True)
            self.assertTrue(hasattr(ds, "RecognizableVisualFeatures"), f"Missing RecognizableVisualFeatures tag in {fpath}")
            self.assertEqual(str(ds.RecognizableVisualFeatures).upper(), "NO", f"Tag not set to NO in {fpath}")


if __name__ == "__main__":
    unittest.main()