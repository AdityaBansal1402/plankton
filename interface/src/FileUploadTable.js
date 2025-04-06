import React, { useState } from "react";
import { BrowserRouter as Router, Route, Routes, useNavigate } from "react-router-dom";
import { useTable } from "react-table";
import Papa from "papaparse";
import * as XLSX from "xlsx";
import "tailwindcss/tailwind.css";

function Home() {
  const [file, setFile] = useState(null);
  const navigate = useNavigate();

  const handleFileChange = (event) => {
    setFile(event.target.files[0]);
  };

  const handleRemoveFile = () => {
    setFile(null);
  };

  const handleSubmit = () => {
    if (file) {
      navigate("/table", { state: { file } });
    }
  };

  return (
    <div className="flex flex-col items-center justify-center h-screen">
      <label className="p-4 border-2 border-dashed cursor-pointer">
        <input type="file" className="hidden" onChange={handleFileChange} accept=".csv,.xlsx" />
        Upload CSV/XLSX
      </label>
      {file && (
        <div className="mt-2 flex items-center">
          <span>{file.name}</span>
          <button className="ml-2 text-red-500" onClick={handleRemoveFile}>
            ✖
          </button>
        </div>
      )}
      <button
        className="mt-4 px-4 py-2 bg-blue-500 text-white rounded"
        onClick={handleSubmit}
        disabled={!file}
      >
        Submit
      </button>
    </div>
  );
}

function TableView({ location }) {
  const file = location?.state?.file;
  const [columns, setColumns] = useState([]);
  const [data, setData] = useState([]);

  React.useEffect(() => {
    if (!file) return;
    const reader = new FileReader();
    reader.onload = ({ target }) => {
      let parsedData;
      if (file.name.endsWith(".csv")) {
        parsedData = Papa.parse(target.result, { header: true }).data;
      } else {
        const workbook = XLSX.read(target.result, { type: "binary" });
        const sheet = workbook.Sheets[workbook.SheetNames[0]];
        parsedData = XLSX.utils.sheet_to_json(sheet);
      }
      if (parsedData.length) {
        setColumns(Object.keys(parsedData[0]));
        setData(parsedData);
      }
    };
    if (file.name.endsWith(".csv")) {
      reader.readAsText(file);
    } else {
      reader.readAsBinaryString(file);
    }
  }, [file]);

  const tableInstance = useTable({
    columns: columns.map((col) => ({ Header: col, accessor: col })),
    data,
  });
  const { getTableProps, getTableBodyProps, headerGroups, rows, prepareRow } = tableInstance;

  return (
    <div className="flex h-screen">
      <div className="w-1/4 p-4">
        <select className="w-full p-2 border rounded">
          {columns.map((col) => (
            <option key={col}>{col}</option>
          ))}
        </select>
      </div>
      <div className="w-3/4 overflow-auto p-4 border-l">
        <table {...getTableProps()} className="w-full border-collapse border">
          <thead>
            {headerGroups.map((headerGroup) => (
              <tr {...headerGroup.getHeaderGroupProps()} className="bg-gray-200">
                {headerGroup.headers.map((column) => (
                  <th {...column.getHeaderProps()} className="border p-2">
                    {column.render("Header")}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody {...getTableBodyProps()}>
            {rows.map((row) => {
              prepareRow(row);
              return (
                <tr {...row.getRowProps()} className="border">
                  {row.cells.map((cell) => (
                    <td {...cell.getCellProps()} className="border p-2">
                      {cell.render("Cell")}
                    </td>
                  ))}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/table" element={<TableView />} />
      </Routes>
    </Router>
  );
}
