/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.catalog.utils;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.http.HttpStatusCode;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Router<I,O> {
    private final Map<String, RequestHandler<I,O>> routes = new LinkedHashMap<>();

    public Router<I,O> add(String route, RequestHandler<I,O> handler){
        routes.put(route, handler);
        return this;
    }

    public Router<I,O> add(String method, String path, RequestHandler<I,O> handler){
        return add(toRoute(method, path), handler);
    }

    public O handle(String route, final I request, final Context context) {
        Optional<String> key = routes.keySet()
                .stream()
                .filter(route::matches)
                .findFirst();
        return routes.get(key.orElseThrow(() ->
                new ApiExceptionBuilder(HttpStatusCode.NOT_FOUND)
                        .message("Not Found")
                        .build()))
                .handleRequest(request, context);
    }

    public O handle(String method, String path, final I request, final Context context) {
        return handle(toRoute(method, path), request, context);
    }

    public static String extract(String src, String pattern) {
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(src);
        return m.find() ? m.group(0) : null;
    }

    private String toRoute(String method, String path) {
        return method + " " + path;
    }
}
