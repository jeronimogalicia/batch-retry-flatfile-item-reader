package com.demo.spring.batch;

import com.demo.spring.batch.model.Player;
import org.springframework.batch.item.ItemWriter;

import java.util.List;

public class MyWriter implements ItemWriter<Player> {

	@Override
	public void write(final List<? extends Player> arg0s) throws Exception {
		for (Player arg0 : arg0s) {
			System.out.println("MyWriter : " + arg0.getFirstName());
		}
	}
}
