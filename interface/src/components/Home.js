import React, { useState } from "react";
import { useNavigate } from "react-router-dom";

function Home() {
  const [file, setFile] = useState(null);
  const navigate = useNavigate();

  const handleFileChange = (event) => {
    const selectedFile = event.target.files[0];
    if (selectedFile) {
      setFile(selectedFile);
    }
  };

  const handleSubmit = () => {
    if (file) {
      navigate("/table-view", { state: { file } });
    }
  };

  return (
    <div className="h-screen flex items-center justify-center bg-gray-100">
      <div className="bg-white p-6 rounded-lg shadow-lg w-96">
        <h2 className="text-xl font-semibold mb-4 text-center">Upload File</h2>
        <input
          type="file"
          accept=".csv, .xlsx"
          onChange={handleFileChange}
          className="block w-full text-sm text-gray-900 border border-gray-300 rounded-lg cursor-pointer focus:outline-none"
        />
        {file && (
          <div className="mt-4 flex items-center justify-between bg-gray-200 p-2 rounded-md">
            <span className="text-sm font-medium">{file.name}</span>
            <button
              className="text-red-600 text-lg"
              onClick={() => setFile(null)}
            >
              ❌
            </button>
          </div>
        )}
        <button
          onClick={handleSubmit}
          disabled={!file}
          className={`mt-4 w-full py-2 text-white font-semibold rounded-md ${
            file
              ? "bg-blue-600 hover:bg-blue-700"
              : "bg-gray-400 cursor-not-allowed"
          }`}
        >
          Submit
        </button>
      </div>
    </div>
  );
}

export default Home;
