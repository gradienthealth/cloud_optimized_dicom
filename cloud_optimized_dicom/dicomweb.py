# Reference: https://www.dicomstandard.org/docs/librariesprovider2/dicomdocuments/dicom/wp-content/uploads/2018/04/dicomweb-cheatsheet.pdf
def handle_dicomweb_request(request: str):
    """
    Handle a dicomweb request of format "GET {s}/studies/{study}/series/{series}"
    """
    print("Received dicomweb request: ", request)
