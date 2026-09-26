package pi

import (
	"bytes"
	"io"
	"mime/multipart"
	"net/http"
)

type limitedRequestBuffer struct {
	bytes.Buffer
	limit int64
}

func (b *limitedRequestBuffer) Write(p []byte) (int, error) {
	if int64(b.Len())+int64(len(p)) > b.limit {
		return 0, &http.MaxBytesError{Limit: b.limit}
	}
	return b.Buffer.Write(p)
}

// A middleware may have parsed multipart before Pi. FileHeader.Clone does not
// own the underlying temporary file, and net/http removes it when ServeHTTP
// returns. Re-encode within the same limit so a late Handler owns its own parts.
func snapshotBody(w http.ResponseWriter, r *http.Request, max int64) ([]byte, string, error) {
	if r.MultipartForm == nil {
		if r.Body == nil {
			return nil, "", nil
		}
		data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, max))
		return data, "", err
	}
	buffer := &limitedRequestBuffer{limit: max}
	writer := multipart.NewWriter(buffer)
	for key, values := range r.MultipartForm.Value {
		for _, value := range values {
			if err := writer.WriteField(key, value); err != nil {
				return nil, "", err
			}
		}
	}
	for _, files := range r.MultipartForm.File {
		for _, file := range files {
			part, err := writer.CreatePart(file.Header)
			if err != nil {
				return nil, "", err
			}
			source, err := file.Open()
			if err != nil {
				return nil, "", err
			}
			_, err = io.Copy(part, source)
			closeErr := source.Close()
			if err != nil {
				return nil, "", err
			}
			if closeErr != nil {
				return nil, "", closeErr
			}
		}
	}
	if err := writer.Close(); err != nil {
		return nil, "", err
	}
	return buffer.Bytes(), writer.FormDataContentType(), nil
}
