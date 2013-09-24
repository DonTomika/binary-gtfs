/**
 * Copyright (C) 2011 Brian Ferris <bdferris@onebusaway.org>
 * 
 * Copyright (C) 2013 Tamás Szincsák <dontomika@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.onebusaway.gtfs_transformer;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.AlreadySelectedException;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.varia.NullAppender;
import org.onebusaway.gtfs_transformer.updates.ProcessFeedStrategy;

public class GtfsTransformerMain {
	public static void main(String[] args) throws IOException {
		BasicConfigurator.configure(new NullAppender());

		try {
			List<File> input = new ArrayList<File>();
			input.add(new File(args[0]));

			GtfsTransformer transformer = new GtfsTransformer();
			transformer.setGtfsInputDirectories(input);

			ProcessFeedStrategy strategy = new ProcessFeedStrategy(new File(
					args[1]));
			transformer.addTransform(strategy);

			System.setOut(new PrintStream(new File(args[2])));

			transformer.run();
		} catch (MissingOptionException ex) {
			System.err.println("Missing argument: " + ex.getMessage());
		} catch (MissingArgumentException ex) {
			System.err.println("Missing argument: " + ex.getMessage());
		} catch (UnrecognizedOptionException ex) {
			System.err.println("Unknown argument: " + ex.getMessage());
		} catch (AlreadySelectedException ex) {
			System.err.println("Argument already selected: " + ex.getMessage());
		} catch (ParseException ex) {
			System.err.println(ex.getMessage());
		} catch (TransformSpecificationException ex) {
			System.err.println("error with transform line: " + ex.getLine());
			System.err.println(ex.getMessage());
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
