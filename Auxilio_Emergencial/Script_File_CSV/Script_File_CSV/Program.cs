using System;
using System.IO;
using System.IO.Compression;

namespace Script_File_CSV
{
    class Program
    {
        static void Main(string[] args)
        {
            string path_origin = @"C:\Users\antoliverjr\Downloads\";
            string path_hist = @"C:\Workspace\DataAnalystics\Aux_Emergencial\historico_zip\";
            string path_file = @"C:\Workspace\DataAnalystics\Aux_Emergencial\csv_files_aux_emergencial\";
            string name_file = @"AuxilioEmergencial.csv";
            string path_full_file = "";

            var aux_emerg_zip = Directory.EnumerateFiles(path_origin, "*ABC_AuxilioEmergencial.zip");
            
            foreach (string zip_file in aux_emerg_zip)
            {
                Console.WriteLine(zip_file);
                string zip_name = zip_file.Substring(path_origin.Length);
                Console.WriteLine(zip_name);
                path_full_file = Path.Combine(path_hist, zip_name);
                Directory.Move(zip_file, path_full_file);
                break;
            }

            Console.WriteLine(path_full_file);

            if(File.Exists(path_full_file))
            {
                ZipFile.ExtractToDirectory(path_full_file, path_file);

                string file_csv = path_full_file.Substring(path_hist.Length);
                file_csv = file_csv.Replace("zip", "csv");

                File.Move(Path.Combine(path_file, file_csv), Path.Combine(path_file, name_file));
            }

            Console.WriteLine("Sem Arquivo");

            Console.ReadLine();
        }
    }
}
