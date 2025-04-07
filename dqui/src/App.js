import { BrowserRouter as Router, Routes, Route } from "react-router-dom";
import Home from "./components/Home";
import TableView from "./components/TableView";

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/table-view" element={<TableView />} />
      </Routes>
    </Router>
  );
}

export default App;
