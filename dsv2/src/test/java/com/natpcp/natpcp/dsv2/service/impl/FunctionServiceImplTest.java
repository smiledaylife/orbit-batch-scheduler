package com.natpcp.natpcp.dsv2.service.impl;

import com.natpcp.natpcp.dsv2.constant.MgCachePrefixEnum;
import com.natpcp.natpcp.dsv2.dao.mapper.FunctionMapper;
import com.natpcp.natpcp.dsv2.service.impl.FunctionServiceImpl;
import com.natpcp.natpcp.dsv2.util.RedisUtils;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * AICode Begin
 * FunctionServiceImpl unit test.
 */
@DisplayName("FunctionServiceImpl Test")
@ExtendWith(MockitoExtension.class)
class FunctionServiceImplTest {

    @Mock
    private RedisUtils redisUtils;

    @Mock
    private FunctionMapper functionMapper;

    @InjectMocks
    private FunctionServiceImpl functionService;

    @DisplayName("updateRedisFunctionUrls: 正常刷新 URL 集合到 Redis 缓存")
    @Test
    void updateRedisFunctionUrls_shouldSetUrls_whenNotEmpty() {
        Set<String> urls = new LinkedHashSet<>(Arrays.asList("/a", "/b", "/c"));
        when(functionMapper.getUrls()).thenReturn(urls);

        functionService.updateRedisFunctionUrls();

        verify(functionMapper).getUrls();
        verify(redisUtils).set(MgCachePrefixEnum.FUNCTION_URLS, urls);
    }

    @DisplayName("updateRedisFunctionUrls: null 集合也写入 Redis")
    @Test
    void updateRedisFunctionUrls_shouldSetNull() {
        when(functionMapper.getUrls()).thenReturn(null);

        functionService.updateRedisFunctionUrls();

        verify(redisUtils).set(MgCachePrefixEnum.FUNCTION_URLS, null);
    }

    @DisplayName("updateRedisFunctionUrls: 空集合也写入 Redis")
    @Test
    void updateRedisFunctionUrls_shouldSetEmpty() {
        when(functionMapper.getUrls()).thenReturn(new LinkedHashSet<>());

        functionService.updateRedisFunctionUrls();

        verify(redisUtils).set(MgCachePrefixEnum.FUNCTION_URLS, new LinkedHashSet<>());
    }

    @DisplayName("updateRedisFunctionUrls: getUrls 异常时向上抛出")
    @Test
    void updateRedisFunctionUrls_shouldThrow_whenMapperFails() {
        when(functionMapper.getUrls()).thenThrow(new RuntimeException("db down"));
        try {
            functionService.updateRedisFunctionUrls();
        } catch (RuntimeException e) {
            assertEquals("db down", e.getMessage());
            verifyNoInteractions(redisUtils);
            return;
        }
        org.junit.jupiter.api.Assertions.fail("expected exception");
    }
}
/*
 * AICode End
 */
