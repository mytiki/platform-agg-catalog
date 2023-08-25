/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.catalog.utils;

public class ApiException extends RuntimeException {
    private final int status;
    private final ApiError error;

    public ApiException(int status, ApiError error) {
        super( status + "-" + error.getMessage());
        this.error = error;
        this.status = status;
    }

    public ApiException(int status, ApiError error, Throwable cause) {
        super(status + "-" + error.getMessage(), cause);
        this.error = error;
        this.status = status;
    }

    public ApiError getError() {
        return error;
    }

    public int getStatus() {
        return status;
    }
}
