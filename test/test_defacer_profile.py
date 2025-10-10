import vedo
import SimpleITK as sitk
import unittest
import os
import shutil
import pydicom
import tempfile
import json
import sys
import tarfile
import urllib.request
# Add luwakx directory to Python path for imports
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'luwakx'))
from anonymize import LuwakAnonymizer
from luwak_logger import setup_logger, get_logger

class TestDefacerProfile(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a temporary output directory
        cls.test_output_dir = "test_output"

        # Path to the decompressed test data directory
        cls.test_data_dir = "test_data_defacer"

        # Check if the test data directory exists
        if not os.path.exists(cls.test_data_dir):

            # URL of the test data archive
            test_data_url = "https://github.com/Simlomb/Test-data-anonymization/releases/download/0.0.1-dicom-files-test/test-dicom-files-2.tar.gz"

            # Download the archive
            archive_path = "test-dicom-files-2.tar.gz"
            urllib.request.urlretrieve(test_data_url, archive_path)

            # Extract the archive
            with tarfile.open(archive_path, "r:gz") as tar:
                # Extract all files directly into the test_data_dir
                for member in tar.getmembers():
                    # Remove the top-level folder from the path
                    member.path = os.path.relpath(member.path, start="test-dicom-files-2")
                    tar.extract(member, path=cls.test_data_dir, filter='data')

            # Clean up the downloaded archive
            os.remove(archive_path)

    @classmethod
    def tearDownClass(cls):
        # Clean up output directory after all tests
        if os.path.exists(cls.test_output_dir):
            pass# shutil.rmtree(cls.test_output_dir)

    @classmethod
    def create_test_config(cls, input_folder, output_folder):
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

    def tearDown(self):
        # Don't clean up immediately - let user see the plot
        print("\n######################END TEST######################")
        # Note: cleanup moved to tearDownClass to allow plot viewing

    def test_defacer_profile(self):    
        # Test the defacer profile method directly
        print("Testing defacer profile method...")
        config_path = self.create_test_config(self.test_data_dir, self.test_output_dir)
        anonymizer = LuwakAnonymizer(config_path)
        processed_files = anonymizer.clean_recognizable_visual_features(self.test_data_dir, self.test_output_dir)
        self.assertTrue(processed_files, "No files processed by defacer!")

        # Plot image.nrrd and image_defaced.nrrd side by side and block until closed
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
                array_cropped = array[x_half:, :, :]
                defaced_array_cropped = defaced_array[x_half:, :, :]
                # Normalize data to [0, 1] for better contrast
                array_norm = (array_cropped - array_cropped.min()) / (array_cropped.max() - array_cropped.min())
                defaced_array_norm = (defaced_array_cropped - defaced_array_cropped.min()) / (defaced_array_cropped.max() - defaced_array_cropped.min())
                spacing = image.GetSpacing()  # (x, y, z) or (width, height, slice)
                # vedo expects spacing in (z, y, x) order for numpy arrays
                spacing = spacing[::-1]
                threshold = 0.45
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
                plt.add(vedo.Text2D("Original", pos="top-left", c="white", bg="black"), at=0)
                plt.add(vedo.Text2D("Defaced", pos="top-left", c="white", bg="black"), at=1)

                # Camera position: move along negative y, looking back at the center
                cam_pos = [center[0], -center[1] - 500, center[2]]  
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
        config_path = self.create_test_config(self.test_data_dir, self.test_output_dir)
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