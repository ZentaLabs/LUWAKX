import zipfile
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

# Add luwakx directory to Python path for imports
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'luwakx'))
from anonymize import LuwakAnonymizer
from utils import has_gpu, download_github_asset_by_tag

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

        # Download CT_Vol_002_STD_face_mask.nii.gz into test_data_defacer/
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
        print("\n######################START TEST######################")

    def test_defacer_profile_should_apply_defacing(self):    
        # Test the defacer profile method directly
        print("Testing defacer profile method...")
        # Simple GPU check
        HAS_GPU = has_gpu()

        if not HAS_GPU:
            useExistingMaskDefacer = os.path.abspath(os.path.join(self.test_data_dir, "CT_Vol_002_STD_face_mask.nrrd"))
            config_path = self.create_test_config(self.test_data_dir, self.test_output_dir, [useExistingMaskDefacer])
        else:
            config_path = self.create_test_config(self.test_data_dir, self.test_output_dir)
        anonymizer = LuwakAnonymizer(config_path)
        processed_files = anonymizer.clean_recognizable_visual_features(self.test_volume_dir, self.test_output_dir)
        self.assertTrue(processed_files, "No files processed by defacer!")
        # Plot image.nrrd and image_defaced.nrrd side by side and block until closed
        if not HAS_GPU:
            return
        
        for root, dirs, files in os.walk(self.test_output_dir):
            if "image.nrrd" in files and "image_defaced.nrrd" in files:
                image_path = os.path.join(root, "image.nrrd")
                defaced_path = os.path.join(root, "image_defaced.nrrd")
                # Read the NRRD files
                image = sitk.ReadImage(image_path)
                defaced = sitk.ReadImage(defaced_path)
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
                return
        self.fail("No image.nrrd and image_defaced.nrrd found in output folders.")

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
        result_files = anonymizer.anonymize()
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