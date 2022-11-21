package au.csiro.pathling.library;

import org.apache.commons.text.CaseUtils;
import org.apache.hadoop.shaded.org.apache.commons.beanutils.BeanUtils;
import javax.annotation.Nonnull;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class PytonUtils {

  @SuppressWarnings("unused")
  @Nonnull
  public static Object toConfigBean(@Nonnull String beanClazz,
      @Nonnull Map<Object, Object> properties)
      throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    System.out.println("XXXX");
    System.out.println("Input: " + properties);
    final Class<?> clazz = Class.forName(beanClazz);
    final Object bean = clazz.getDeclaredMethod("defaults").invoke(clazz);
    final Map<String, Object> translatedProperties = properties.entrySet().stream()
        .filter(e -> e.getValue() != null && e.getKey() instanceof String)
        .collect(Collectors.toUnmodifiableMap(
            e -> CaseUtils.toCamelCase((String) e.getKey(), false, '_' ), Entry::getValue));

    System.out.println("Translated: " + translatedProperties);
    BeanUtils.populate(bean, translatedProperties);
    return bean;
  }
}
