package fr.an.tools.jdbclogger.util;

import java.lang.reflect.Method;

/**
 * reflection utility methods
 *
 */
public final class ReflectUtils {

	public static <T> Class<T> safeLoadClassOrNull(String className) {
		String resourceName = className.replace(".", "/") + ".class";
		if (ReflectUtils.class.getClassLoader().getResource(resourceName) == null) {
			return null;
		}
		try {
			@SuppressWarnings("unchecked")
			Class<T> res = (Class<T>) Class.forName(className);
			return res;
		} catch (ClassNotFoundException ex) {
			return null;
		}
	}

	public static <T> Class<T> loadClass(String className) {
		try {
			@SuppressWarnings("unchecked")
			Class<T> res = (Class<T>) Class.forName(className);
			return res;
		} catch (ClassNotFoundException ex) {
			throw new RuntimeException("Failed Class.forName(\"" + className + "\")", ex);
		}
	}
	
	public static <T> T newInstance(Class<T> clss) {
		try {
			return clss.newInstance();
		} catch (InstantiationException ex) {
			throw new RuntimeException("Failed class.newInstance()", ex);
		} catch (IllegalAccessException ex) {
			throw new RuntimeException("Failed class.newInstance()", ex);
		}
	}

	public static <T> T newInstance(String className) {
		Class<T> clss = loadClass(className);
		return newInstance(clss);
	}
	
	public static <T> Object invokeMethod(Object obj, String methodName, Class<T> argType, T argument) {
		try {
			Class<?> clss = obj.getClass();
			Method meth = clss.getMethod(methodName, argType);
			return meth.invoke(obj, argument);
		} catch (Exception ex) {
			throw new RuntimeException("Failed invoke obj." + methodName + "(" + argument + ")", ex);
		}
	}

}
