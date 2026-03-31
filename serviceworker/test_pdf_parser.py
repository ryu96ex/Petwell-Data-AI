import io
from pypdf import PdfReader

PDF_PATH = "test_medical_record.pdf"  # change if needed

def extract_text_from_pdf_bytes(pdf_bytes: bytes) -> str:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    text_parts = []
    for i, page in enumerate(reader.pages):
        text = page.extract_text() or ""
        text_parts.append(f"\n--- Page {i+1} ---\n{text}")
    return "\n".join(text_parts).strip()

with open(PDF_PATH, "rb") as f:
    pdf_bytes = f.read()

text = extract_text_from_pdf_bytes(pdf_bytes)

print("Extracted text length:", len(text))
print("\nPreview:\n")
print(text[:2000])