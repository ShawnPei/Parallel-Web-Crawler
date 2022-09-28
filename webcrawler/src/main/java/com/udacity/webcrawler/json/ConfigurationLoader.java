package com.udacity.webcrawler.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.imageio.IIOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * A static utility class that loads a JSON configuration file.
 */
public final class ConfigurationLoader {

  private final Path path;

  /**
   * Create a {@link ConfigurationLoader} that loads configuration from the given {@link Path}.
   */
  public ConfigurationLoader(Path path) {
    this.path = Objects.requireNonNull(path);
  }

  /**
   * Loads configuration from this {@link ConfigurationLoader}'s path
   *
   * @return the loaded {@link CrawlerConfiguration}.
   */
  public CrawlerConfiguration load() throws IOException {
    // TODO: Fill in this method.

    //first we creat a bufferedreader to read in the path
    try(Reader reader = Files.newBufferedReader(path)){
      CrawlerConfiguration crawlerConfiguration = read(reader);
      reader.close();
      return crawlerConfiguration;
      //as in the path there are only JSON files, so we use the method we wrote to convert
      //JSON TO JAVA
    }catch(IOException e){
      e.printStackTrace();
    }
    return new CrawlerConfiguration.Builder().build();
  }

  /**
   * Loads crawler configuration from the given reader.
   *
   * @param reader a Reader pointing to a JSON string that contains crawler configuration.
   * @return a crawler configuration
   */
  public static CrawlerConfiguration read(Reader reader) {
    // This is here to get rid of the unused variable warning.
    Objects.requireNonNull(reader);
    // TODO: Fill in this method
    //To creat an ObjectMapper
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
    try{
      //set as final is to ensure that the read-in values can't be changed.
      //the reader in readValue represent the address to a JSON file, and the class file refers to the JSON file which
      //needs to be converted to java file.
      final CrawlerConfiguration crawlerConfigurationBuilder = objectMapper.readValue(reader, CrawlerConfiguration.class);
      return crawlerConfigurationBuilder;
    }catch(IOException e){//this is because when writing the line above, idea will inform you of this exception.
      e.printStackTrace();//this will help you trace the exception
      return null;
    }


  }
}
